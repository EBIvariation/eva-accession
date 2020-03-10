/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.clustering.batch.io;

import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.clustering.batch.processors.VariantToSubmittedVariantProcessor.REFERENCE_SEQUENCE_ACCESSION;

public class SubmittedVariantWriter implements ItemWriter<SubmittedVariantEntity> {

    private MongoTemplate mongoTemplate;

    private Function<IClusteredVariant, String> hashingFunction;

    private List<Long> accessions;

    private Iterator<Long> iterator;

    private Map<String, Long> assignedAccessions;

    private Map<String, Long> mongoAssignedAccessions;

    public SubmittedVariantWriter(MongoTemplate mongoTemplate) {
        this.mongoTemplate = mongoTemplate;
        hashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        initializeAccessions();
        this.iterator = this.accessions.iterator();
        this.assignedAccessions = new HashMap<>();
        this.mongoAssignedAccessions = new HashMap<>();
    }

    private void initializeAccessions() {
        this.accessions = new ArrayList<>();
        long firstAccession = 1000L;
        long lastAccession = 1010L;
        for (long i = firstAccession; i <= lastAccession; i++) {
            accessions.add(i);
        }
    }

    @Override
    public void write(List<? extends SubmittedVariantEntity> clusteredSubmittedVariants) throws Exception {
        try {
            assignedAccessions.clear();
            //Write new Clustered Variants in Mongo and get existing ones (Done by accessioning service)
            getOrCreate(clusteredSubmittedVariants);
            //Update Submitted Variants "rs" field
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                  SubmittedVariantEntity.class);
            for (SubmittedVariantEntity submittedVariantEntity : clusteredSubmittedVariants) {
                Query query = query(where("_id").is(submittedVariantEntity.getId()));
                Update update = new Update();
                update.set("rs", getRs(submittedVariantEntity));
                bulkOperations.updateOne(query, update);
            }
            bulkOperations.execute();
        } catch (BulkOperationException e) {
            throw e;
        }
    }

    /**
     * RS accession are generated if they don't exits (based on hash)
     * mongoAssignedAccessions: simulate the mongo DB
     * assignedAccessions: is the cache to keep track of the accessions used in the current chunk
     */
    private void getOrCreate(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        for (SubmittedVariantEntity submittedVariantEntity : submittedVariantEntities) {
            String hash = getClusteredVariantHash(submittedVariantEntity);
            if (!mongoAssignedAccessions.containsKey(hash)) {
                Long generatedClusteredVariantAccession = iterator.next();
                mongoAssignedAccessions.put(hash, generatedClusteredVariantAccession);
                assignedAccessions.put(hash, generatedClusteredVariantAccession);
            } else {
                assignedAccessions.put(hash, mongoAssignedAccessions.get(hash));
            }
        }
    }

    private String getClusteredVariantHash(SubmittedVariantEntity submittedVariantEntity) {
        ClusteredVariant clusteredVariant = new ClusteredVariant(REFERENCE_SEQUENCE_ACCESSION,
                                                                 submittedVariantEntity.getTaxonomyAccession(),
                                                                 submittedVariantEntity.getContig(),
                                                                 submittedVariantEntity.getStart(),
                                                                 getVariantType(
                                                                         submittedVariantEntity.getReferenceAllele(),
                                                                         submittedVariantEntity.getAlternateAllele()),
                                                                 submittedVariantEntity.isValidated(),
                                                                 submittedVariantEntity.getCreatedDate());
        String hash = hashingFunction.apply(clusteredVariant);
        return hash;
    }

    private Long getRs(SubmittedVariantEntity submittedVariantEntity) {
        String hash = getClusteredVariantHash(submittedVariantEntity);
        return assignedAccessions.get(hash);
    }

    private VariantType getVariantType(String reference, String alternate) {
        VariantType variantType = VariantClassifier.getVariantClassification(reference, alternate, 0);
        return variantType;
    }
}
