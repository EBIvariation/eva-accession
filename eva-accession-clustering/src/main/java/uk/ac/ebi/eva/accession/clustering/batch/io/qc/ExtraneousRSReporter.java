/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.io.qc;

import com.mongodb.MongoBulkWriteException;
import org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.QCMongoCollections.qcRSIdInSS;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;

import javax.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;

public class ExtraneousRSReporter implements ItemWriter<ClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ExtraneousRSReporter.class);

    private static final String IDAttribute = "_id";

    private final String assemblyAccession;

    private final MongoTemplate mongoTemplate;

    public ExtraneousRSReporter(String assemblyAccession, MongoTemplate mongoTemplate) {
        this.assemblyAccession = assemblyAccession;
        this.mongoTemplate = mongoTemplate;
    }

    @Override
    public void write(@Nonnull List<? extends ClusteredVariantEntity> clusteredVariantEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        reportExtraneousRS(clusteredVariantEntities);
    }

    private void reportExtraneousRS(List<? extends ClusteredVariantEntity> clusteredVariantEntities) {
        String assemblyAccessionPrefix = QCMongoCollections.getAssemblyAccessionPrefix(this.assemblyAccession);
        List<String> idsFromRSIDCollection = clusteredVariantEntities.stream().map(
                entity -> assemblyAccessionPrefix + entity.getAccession()).distinct().collect(Collectors.toList());
        Criteria[]  criteriaToLookupIDs =
                idsFromRSIDCollection.stream().map(id -> where(IDAttribute).is(id)).toArray(Criteria[]::new);
        if (criteriaToLookupIDs.length == 0) return;
        Query queryToLookupIDs = new Query(new Criteria().orOperator(criteriaToLookupIDs));
        List<String> idsInSSIDCollection = this.mongoTemplate.find(queryToLookupIDs, qcRSIdInSS.class)
                .stream().map(qcRSIdInSS::getId).collect(Collectors.toList());

        Arrays.stream(CollectionUtils.subtract(idsFromRSIDCollection, idsInSSIDCollection).toArray())
                .map(Object::toString)
                .forEach(extraneousRS ->
                        logger.error("RS ID rs{} was not assigned to any SS in the assembly {}",
                                extraneousRS.replace(assemblyAccessionPrefix, ""), this.assemblyAccession));
    }
}
