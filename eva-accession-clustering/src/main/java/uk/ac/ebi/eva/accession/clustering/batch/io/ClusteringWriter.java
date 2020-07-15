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

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.Priority;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.prioritise;

/**
 * This writer has two parts:
 * 1. Use the accessioning service to generate new RS IDs or get existing ones
 * 2. Update the submitted variants to include the "rs" field with the generated/retrieved accessions
 */
public class ClusteringWriter implements ItemWriter<SubmittedVariantEntity> {

    public static final String ACCESSION_KEY = "accession";

    public static final String RS_KEY = "rs";

    private MongoTemplate mongoTemplate;

    private ClusteredVariantAccessioningService clusteredService;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    private Map<String, Long> assignedAccessions;

    private Long accessioningMonotonicInitSs;

    private Long accessioningMonotonicInitRs;

    public ClusteringWriter(MongoTemplate mongoTemplate,
                            ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                            Long accessioningMonotonicInitSs,
                            Long accessioningMonotonicInitRs) {
        this.mongoTemplate = mongoTemplate;
        this.clusteredService = clusteredVariantAccessioningService;
        this.clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        this.assignedAccessions = new HashMap<>();
        Assert.notNull(accessioningMonotonicInitSs, "accessioningMonotonicInitSs must not be null. Check autowiring.");
        this.accessioningMonotonicInitSs = accessioningMonotonicInitSs;
        this.accessioningMonotonicInitRs = accessioningMonotonicInitRs;
    }

    @Override
    public void write(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException {
        assignedAccessions.clear();

        // Write new Clustered Variants in mongo and get existing ones. May merge clustered variants
        getOrCreateClusteredVariantAccessions(submittedVariantEntities);

        // Update submitted variants "rs" field
        clusterSubmittedVariants(submittedVariantEntities);
    }

    private void getOrCreateClusteredVariantAccessions(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws AccessionCouldNotBeGeneratedException {
        List<ClusteredVariant> clusteredVariants = submittedVariantEntities.stream()
                                                                           .map(this::toClusteredVariant)
                                                                           .collect(Collectors.toList());
        if (!clusteredVariants.isEmpty()) {
            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers =
                    clusteredService.getOrCreate(clusteredVariants);
            accessionWrappers.forEach(x -> assignedAccessions.put(x.getHash(), x.getAccession()));
        }
        checkForMerges(submittedVariantEntities);
    }

    private ClusteredVariant toClusteredVariant(SubmittedVariantEntity submittedVariantEntity) {
        ClusteredVariant clusteredVariant = new ClusteredVariant(submittedVariantEntity.getReferenceSequenceAccession(),
                                                                 submittedVariantEntity.getTaxonomyAccession(),
                                                                 submittedVariantEntity.getContig(),
                                                                 submittedVariantEntity.getStart(),
                                                                 getVariantType(
                                                                         submittedVariantEntity.getReferenceAllele(),
                                                                         submittedVariantEntity.getAlternateAllele()),
                                                                 submittedVariantEntity.isValidated(),
                                                                 submittedVariantEntity.getCreatedDate());
        return clusteredVariant;
    }

    private VariantType getVariantType(String reference, String alternate) {
        // TODO jmmut: this is wrong. Won't classify INDELs properly as the VariantClassifier needs a correct snpclass
        VariantType variantType = VariantClassifier.getVariantClassification(reference, alternate, 0);
        return variantType;
    }

    private void checkForMerges(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        for (SubmittedVariantEntity submittedVariant : submittedVariantEntities) {
            if (submittedVariant.getClusteredVariantAccession() != null) {
                String hash = clusteredHashingFunction.apply(toClusteredVariant(submittedVariant));
                Long accessionInDatabase = assignedAccessions.get(hash);
                if (!submittedVariant.getClusteredVariantAccession().equals(accessionInDatabase)) {
                    merge(submittedVariant.getClusteredVariantAccession(), hash, accessionInDatabase);
                }
            }
        }
    }

    private void merge(Long providedAccession, String hash, Long accessionInDatabase) {
        Priority prioritised = prioritise(providedAccession, accessionInDatabase);

        Query queryClustered = query(where(ACCESSION_KEY).is(prioritised.accessionToBeMerged));
        List<? extends ClusteredVariantEntity> clusteredVariantToMerge =
                mongoTemplate.find(queryClustered, getClusteredVariantCollection(prioritised.accessionToBeMerged));

        List<? extends ClusteredVariantEntity> clusteredVariantToKeep =
                mongoTemplate.find(query(where(ACCESSION_KEY).is(prioritised.accessionToKeep)),
                                   getClusteredVariantCollection(prioritised.accessionToKeep));

        if (isMultimap(clusteredVariantToMerge) || isMultimap(clusteredVariantToKeep)) {
            // multimap! don't merge. see isMultimap() below for more details
            return;
        }

        assignedAccessions.put(hash, prioritised.accessionToKeep);

        // write operations for clustered variant being merged
        List<ClusteredVariantOperationEntity> operations =
                clusteredVariantToMerge.stream()
                                       .map(c -> buildClusteredOperation(c, prioritised.accessionToKeep))
                                       .collect(Collectors.toList());
        mongoTemplate.insert(operations, getClusteredOperationCollection(prioritised.accessionToBeMerged));

        mongoTemplate.updateMulti(queryClustered, update(ACCESSION_KEY, prioritised.accessionToKeep),
                                  getClusteredVariantCollection(prioritised.accessionToBeMerged));

        // Update submitted variants linked to the clustered variant we just merged.
        // This has to happen for both EVA and dbsnp SS because previous cross merges might have happened.
        updateSubmittedVariants(prioritised, SubmittedVariantEntity.class, SubmittedVariantOperationEntity.class);
        updateSubmittedVariants(prioritised, DbsnpSubmittedVariantEntity.class,
                                DbsnpSubmittedVariantOperationEntity.class);
    }

    private Class<? extends ClusteredVariantEntity> getClusteredVariantCollection(Long accession) {
        return isEvaClusteredAccession(accession) ? ClusteredVariantEntity.class : DbsnpClusteredVariantEntity.class;
    }

    private boolean isEvaClusteredAccession(Long accession) {
        return accession >= accessioningMonotonicInitRs;
    }

    /**
     * In EVA-2003 we decided not to merge any RS ID that maps to several places in the same assembly
     * (mapping weight > 1) as this might be a signal of a low quality variant, and merging other "real" variants
     * in the same RS would just make things more complicated.
     *
     * Note that for submitted variants the test is not this simple, as 1:1000:A:T and 1:1000:A:G can be present in the
     * same assembly and still not classify as multimap.
     */
    private boolean isMultimap(List<? extends ClusteredVariantEntity> clusteredVariantLoci) {
        int assembliesCount = clusteredVariantLoci.stream()
                                                  .map(ClusteredVariantEntity::getAssemblyAccession)
                                                  .collect(Collectors.toSet())
                                                  .size();
        return assembliesCount < clusteredVariantLoci.size();
    }

    private Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
    getClusteredOperationCollection(Long accession) {
        return isEvaClusteredAccession(accession) ?
                ClusteredVariantOperationEntity.class : DbsnpClusteredVariantOperationEntity.class;
    }

    private ClusteredVariantOperationEntity buildClusteredOperation(ClusteredVariantEntity originalClusteredVariant,
                                                                    Long clusteredVariantMergedInto) {
        ClusteredVariantInactiveEntity inactiveEntity = new ClusteredVariantInactiveEntity(originalClusteredVariant);

        Long originalAccession = originalClusteredVariant.getAccession();
        String reason = "Original rs" + originalAccession + " was merged into rs" + clusteredVariantMergedInto + ".";

        ClusteredVariantOperationEntity operation = new ClusteredVariantOperationEntity();
        operation.fill(EventType.MERGED, originalAccession, clusteredVariantMergedInto, reason,
                       Collections.singletonList(inactiveEntity));
        return operation;
    }

    /**
     * This function updates the clustered variant accession (rs) of submitted variants when the rs makes a
     * collision with another rs and they have to be merged.
     */
    private void updateSubmittedVariants(
            Priority prioritised,
            Class<? extends SubmittedVariantEntity> submittedVariantCollection,
            Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                    submittedOperationCollection) {
        Query querySubmitted = query(where(RS_KEY).is(prioritised.accessionToBeMerged));
        List<? extends SubmittedVariantEntity> svToUpdate =
                mongoTemplate.find(querySubmitted, submittedVariantCollection);

        Update update = new Update();
        update.set(RS_KEY, prioritised.accessionToKeep);
        mongoTemplate.updateMulti(querySubmitted, update, submittedVariantCollection);

        if (!svToUpdate.isEmpty()) {
            List<SubmittedVariantOperationEntity> operations =
                    svToUpdate.stream()
                              .map(sv -> buildSubmittedOperation(sv, prioritised.accessionToKeep))
                              .collect(Collectors.toList());
            mongoTemplate.insert(operations, submittedOperationCollection);
        }
    }

    private SubmittedVariantOperationEntity buildSubmittedOperation(SubmittedVariantEntity originalSubmittedVariant,
                                                                    Long clusteredVariantMergedInto) {
        SubmittedVariantInactiveEntity inactiveEntity = new SubmittedVariantInactiveEntity(originalSubmittedVariant);

        Long originalClusteredVariant = originalSubmittedVariant.getClusteredVariantAccession();
        String reason = "Original rs" + originalClusteredVariant + " was merged into rs" + clusteredVariantMergedInto + ".";

        Long accession = originalSubmittedVariant.getAccession();
        SubmittedVariantOperationEntity operation = new SubmittedVariantOperationEntity();

        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // anything. We are updating the submitted variant, changing its rs field
        operation.fill(EventType.UPDATED, accession, null, reason, Collections.singletonList(inactiveEntity));
        return operation;
    }

    /**
     * This function assigns a clustered variant accession (rs) to the submitted variants that didn't have any.
     */
    private void clusterSubmittedVariants(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              SubmittedVariantEntity.class);
        BulkOperations dbsnpBulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                   DbsnpSubmittedVariantEntity.class);
        long numUpdates = 0;
        long numDbsnpUpdates = 0;
        for (SubmittedVariantEntity submittedVariantEntity : submittedVariantEntities) {
            if (submittedVariantEntity.getClusteredVariantAccession() != null) {
                // no need to update the rs of a submitted variant that already has the correct rs
                continue;
            }
            Query query = query(where("_id").is(submittedVariantEntity.getId()));
            Update update = new Update();
            update.set(RS_KEY, getClusteredVariantAccession(submittedVariantEntity));
            if (isEvaSubmittedVariant(submittedVariantEntity)) {
                bulkOperations.updateOne(query, update);
                ++numUpdates;
            } else {
                dbsnpBulkOperations.updateOne(query, update);
                ++numDbsnpUpdates;
            }
        }
        if (numUpdates > 0) {
            bulkOperations.execute();
        }
        if (numDbsnpUpdates > 0) {
            dbsnpBulkOperations.execute();
        }
    }

    private boolean isEvaSubmittedVariant(SubmittedVariantEntity submittedVariant) {
        return submittedVariant.getAccession() >= accessioningMonotonicInitSs;
    }

    private Long getClusteredVariantAccession(SubmittedVariantEntity submittedVariantEntity) {
        String hash = getClusteredVariantHash(submittedVariantEntity);
        return assignedAccessions.get(hash);
    }

    private String getClusteredVariantHash(SubmittedVariantEntity submittedVariantEntity) {
        ClusteredVariant clusteredVariant = toClusteredVariant(submittedVariantEntity);
        String hash = clusteredHashingFunction.apply(clusteredVariant);
        return hash;
    }
}
