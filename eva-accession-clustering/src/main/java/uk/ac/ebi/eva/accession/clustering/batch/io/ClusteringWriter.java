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
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
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
import static org.springframework.data.mongodb.core.query.Update.*;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.Priority;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.prioritise;

/**
 * This writer has two parts:
 * 1. Use the accessioning service to generate new RS IDs or get existing ones
 * 2. Update the submitted variants to include the "rs" field with the generated/retrieved accessions
 */
public class ClusteringWriter implements ItemWriter<SubmittedVariantEntity> {

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
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException, AccessionMergedException,
            AccessionDoesNotExistException, AccessionDeprecatedException {
        assignedAccessions.clear();

        //Write new Clustered Variants in mongo and get existing ones
        getOrCreateClusteredVariantAccessions(submittedVariantEntities);

        //Update submitted variants "rs" field
        updateSubmittedVariants(submittedVariantEntities);
    }

    private void getOrCreateClusteredVariantAccessions(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws AccessionCouldNotBeGeneratedException, AccessionMergedException, AccessionDoesNotExistException,
            AccessionDeprecatedException {
        List<ClusteredVariant> clusteredVariants = submittedVariantEntities.stream()
                                                                           .map(this::toClusteredVariant)
                                                                           .collect(Collectors.toList());
        if (!clusteredVariants.isEmpty()) {
            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers =
                    clusteredService.getOrCreate(clusteredVariants);
            accessionWrappers.forEach(x -> assignedAccessions.put(x.getHash(), x.getAccession()));
        }
        for (SubmittedVariantEntity submittedVariant : submittedVariantEntities) {
            if (submittedVariant.getClusteredVariantAccession() != null) {
                String hash = clusteredHashingFunction.apply(toClusteredVariant(submittedVariant));
                Long accessionInDatabase = assignedAccessions.get(hash);
                if (!submittedVariant.getClusteredVariantAccession().equals(accessionInDatabase)) {
                    merge(submittedVariant, submittedVariant.getClusteredVariantAccession(), hash, accessionInDatabase);
                }
            }
        }
    }

    private void merge(SubmittedVariantEntity submittedVariant,
                       Long providedAccession, String hash, Long accessionInDatabase)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {

        Priority prioritised = prioritise(providedAccession, accessionInDatabase);
        assignedAccessions.put(hash, prioritised.accessionToKeep);
//        clusteredService.mergeKeepingEntries(prioritised.accessionToBeMerged, prioritised.accessionToKeep,
//                                             "Clustering pipeline detected that these clustered variants were the same."
//        );

        // write operations for clustered variant being merged
        Query queryClustered = query(where("accession").is(prioritised.accessionToBeMerged));
        List<? extends ClusteredVariantEntity> cvToMerge =
                mongoTemplate.find(queryClustered, getClusteredVariantCollection(prioritised.accessionToBeMerged));
        List<ClusteredVariantOperationEntity> operations =
                cvToMerge.stream()
                         .map(c -> buildClusteredOperation(c, prioritised.accessionToKeep))
                         .collect(Collectors.toList());
        mongoTemplate.insert(operations, getClusteredOperationCollection(prioritised.accessionToBeMerged));

        mongoTemplate.updateMulti(queryClustered, update("accession", prioritised.accessionToKeep),
                                  getClusteredVariantCollection(prioritised.accessionToBeMerged));

        // update submitted variants linked to the clustered variant we just merged
        // TODO: if there are several ss or rs in the same assembly, don't merge anything
        updateSubmittedVariants(submittedVariant, prioritised, SubmittedVariantEntity.class,
                                SubmittedVariantOperationEntity.class);
        updateSubmittedVariants(submittedVariant, prioritised, DbsnpSubmittedVariantEntity.class,
                                DbsnpSubmittedVariantOperationEntity.class);
    }

    private void updateSubmittedVariants(
            SubmittedVariantEntity submittedVariant,
            Priority prioritised,
            Class<? extends SubmittedVariantEntity> submittedVariantCollection,
            Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                    submittedOperationCollection) {
        Query querySubmitted = query(where("rs").is(prioritised.accessionToBeMerged));
        List<? extends SubmittedVariantEntity> svToUpdate =
                mongoTemplate.find(querySubmitted, submittedVariantCollection);

        Update update = new Update();
        update.set("rs", prioritised.accessionToKeep);
        mongoTemplate.updateMulti(querySubmitted, update, submittedVariantCollection);

        if (!svToUpdate.isEmpty()) {
            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                  submittedOperationCollection);
            for (SubmittedVariantEntity submittedVariantEntity : svToUpdate) {
                bulkOperations.insert(buildSubmittedOperation(submittedVariantEntity, prioritised.accessionToKeep));
            }

            bulkOperations.execute();
        }
    }

    private Class<? extends SubmittedVariantEntity> getSubmittedVariantCollection(
            SubmittedVariantEntity submittedVariant) {
        return isEvaSubmittedVariant(submittedVariant) ?
                SubmittedVariantEntity.class : DbsnpSubmittedVariantEntity.class;
    }

    private boolean isEvaSubmittedVariant(SubmittedVariantEntity submittedVariant) {
        return submittedVariant.getAccession() >= accessioningMonotonicInitSs;
    }

    private Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
    getSubmittedOperationCollection(SubmittedVariantEntity submittedVariant) {
        return isEvaSubmittedVariant(submittedVariant) ?
                SubmittedVariantOperationEntity.class : DbsnpSubmittedVariantOperationEntity.class;
    }

    private Class<? extends ClusteredVariantEntity> getClusteredVariantCollection(Long accession) {
        return isEvaClusteredAccession(accession) ? ClusteredVariantEntity.class : DbsnpClusteredVariantEntity.class;
    }

    private boolean isEvaClusteredAccession(Long accession) {
        return accession >= accessioningMonotonicInitRs;
    }

    private Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
    getClusteredOperationCollection(Long accession) {
        return isEvaClusteredAccession(accession) ?
                ClusteredVariantOperationEntity.class : DbsnpClusteredVariantOperationEntity.class;
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

    /**
     * This function assigns a clustered variant accession (rs) to the submitted variants that didn't have any.
     *
     * Note that there's a similar scenario that is handled in another function: in case that the rs that should be
     * assigned to a submitted variant makes a collision with another rs
     * and they get merged, the submitted variants whose rs was merged need to be updated too.
     */
    private void updateSubmittedVariants(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
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
            update.set("rs", getClusteredVariantAccession(submittedVariantEntity));
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
