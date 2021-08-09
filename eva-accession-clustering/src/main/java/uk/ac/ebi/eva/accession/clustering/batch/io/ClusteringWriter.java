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
import htsjdk.samtools.util.StringUtil;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.util.Assert;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
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
 *
 * Some edge cases take into account if a clustered variant is multimap. The definition of multimap variants that this
 * class uses is "clustered variants whose mapWeight is 2 or greater". Another definition is "clustered variants
 * whose accession maps several times in the same assembly". Although both definitions should yield the same
 * set of variants, the check for the second definition is less efficient and less accurate: both the active and the
 * deprecated/merged collections should be queried, and we lost clusteredVariantOperations during the
 * deprecation pipeline in the dbSNP import due to a bug.
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

    private ClusteringCounts clusteringCounts;

    public ClusteringWriter(MongoTemplate mongoTemplate,
                            ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                            Long accessioningMonotonicInitSs,
                            Long accessioningMonotonicInitRs,
                            ClusteringCounts clusteringCounts) {
        this.mongoTemplate = mongoTemplate;
        this.clusteredService = clusteredVariantAccessioningService;
        this.clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        this.assignedAccessions = new HashMap<>();
        Assert.notNull(accessioningMonotonicInitSs, "accessioningMonotonicInitSs must not be null. Check autowiring.");
        this.accessioningMonotonicInitSs = accessioningMonotonicInitSs;
        this.accessioningMonotonicInitRs = accessioningMonotonicInitRs;
        this.clusteringCounts = clusteringCounts;
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
        List<IClusteredVariant> processedClusteredVariants = processClusteredVariantsWhereNoRSExists(submittedVariantEntities);

        List<ClusteredVariant> clusteredVariants = submittedVariantEntities.stream()
                                                                           .map(this::toClusteredVariant)
                                                                           .collect(Collectors.toList());
        //remove all variants already processed by processClusteredVariantsNoRSExists
        clusteredVariants.removeAll(processedClusteredVariants);

        if (!clusteredVariants.isEmpty()) {
            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers =
                    clusteredService.getOrCreate(clusteredVariants);

            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionNoMultimap =
                    excludeMultimaps(accessionWrappers);

            accessionNoMultimap.forEach(x -> assignedAccessions.put(x.getHash(), x.getAccession()));

            long newAccessions = accessionWrappers.stream().filter(GetOrCreateAccessionWrapper::isNewAccession).count();
            clusteringCounts.addClusteredVariantsCreated(newAccessions);
        }
        checkForMerges(submittedVariantEntities);
    }

    /**
     * This method handles a specific scenario in clustering where an already clustered variant is remapped and
     * no matching rs id exist. In this case, clustered variant should continue using its existing rs id and an entry
     * needs to be made ClusteredVariantEntity collection.
     *
     *                     SubmittedVariantEntity                                                           ClusteredVariantEntity
     * SS	HASH	        RS	ASM	    STUDY	CONTIG	POS	REF	ALT                             RS	HASH	        ASM	    POS	CONTIG	TYPE
     * 500	ASM1_Chr1_1_A_T	300	ASM1	PRJEB1	Chr1	1	A	T    (original)                 300	ASM1_Chr1_1_SNV	ASM1	1	Chr1	SNV
     * 500	ASM2_Chr1_2_A_T	300	ASM2	PRJEB1	Chr1	2	A	T    (remapped to asm2)
     *
     * No RS id exist for the HASH ASM2_Chr1_2_A_T,	but it has an existing rs id of 300, which it should continue to use.
     * It should not generate a new rs id for this situation as they will get merged right after generation and will be wasted
     *
     *         ClusteredVariantEntity (after operation)
     *          RS	HASH	        ASM	    POS	CONTIG	TYPE
     *          300	ASM1_Chr1_1_SNV	ASM1	1	Chr1	SNV
     *          300	ASM2_Chr1_2_SNV	ASM1	1	Chr1	SNV
     */
    private List<IClusteredVariant> processClusteredVariantsWhereNoRSExists(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        List<SubmittedVariantEntity> submittedVariantWithRS = submittedVariantEntities.stream()
                .filter(v -> v.getClusteredVariantAccession() != null)
                .filter(v -> !StringUtil.isBlank(v.getRemappedFrom()))
                .collect(Collectors.toList());
        List<ClusteredVariantEntity> clusteredVariantEntityNoRSExists = getClusteredVariantEntityNoRSExists(submittedVariantWithRS);
        mongoTemplate.insert(clusteredVariantEntityNoRSExists, ClusteredVariantEntity.class);
        clusteringCounts.addClusteredVariantsRemapped(clusteredVariantEntityNoRSExists.size());
        return clusteredVariantEntityNoRSExists.stream()
                .map(cve -> cve.getModel())
                .collect(Collectors.toList());
    }

    private List<ClusteredVariantEntity> getClusteredVariantEntityNoRSExists(List<? extends SubmittedVariantEntity> submittedVariantWithRS) {
        List<IClusteredVariant> clusteredVariantList = submittedVariantWithRS.stream()
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
        List<String> existingClusteredVariantsHashes = clusteredService.get(clusteredVariantList).stream()
                .map(AccessionWrapper::getHash)
                .collect(Collectors.toList());

        return submittedVariantWithRS.stream()
                .filter(sve -> !existingClusteredVariantsHashes.contains(toClusteredVariantEntity(sve).getHashedMessage()))
                .map(this::toClusteredVariantEntity)
                .collect(Collectors.toList());
    }

    private ClusteredVariantEntity toClusteredVariantEntity(SubmittedVariantEntity submittedVariantEntity) {
        return new ClusteredVariantEntity(submittedVariantEntity.getClusteredVariantAccession(), getClusteredVariantHash(submittedVariantEntity),
                toClusteredVariant(submittedVariantEntity));
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
        VariantType variantType = VariantClassifier.getVariantClassification(reference, alternate);
        return variantType;
    }

    private void checkForMerges(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        for (SubmittedVariantEntity submittedVariant : submittedVariantEntities) {
            if (submittedVariant.getClusteredVariantAccession() != null && submittedVariant.getRemappedFrom() != null) {
                String hash = clusteredHashingFunction.apply(toClusteredVariant(submittedVariant));
                Long accessionInDatabase = assignedAccessions.get(hash);
                //accessionInDatabase will be null if it was excluded for being a multimap
                if (accessionInDatabase != null &&
                        !submittedVariant.getClusteredVariantAccession().equals(accessionInDatabase)) {
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
        clusteringCounts.addClusteredVariantsMergeOperationsWritten(clusteredVariantToMerge.size());

        mongoTemplate.updateMulti(queryClustered, update(ACCESSION_KEY, prioritised.accessionToKeep),
                                  getClusteredVariantCollection(prioritised.accessionToBeMerged));
        clusteringCounts.addClusteredVariantsUpdated(clusteredVariantToMerge.size());

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
    private boolean isMultimap(List<? extends IClusteredVariant> clusteredVariants) {
        return clusteredVariants.stream().anyMatch(cv -> cv.getMapWeight() != null && cv.getMapWeight() > 1);
    }

    private boolean isMultimap(IClusteredVariant clusteredVariant) {
        return isMultimap(Collections.singletonList(clusteredVariant));
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
        clusteringCounts.addSubmittedVariantsUpdatedRs(svToUpdate.size());

        if (!svToUpdate.isEmpty()) {
            List<SubmittedVariantOperationEntity> operations =
                    svToUpdate.stream()
                              .map(sv -> buildSubmittedOperation(sv, prioritised.accessionToKeep))
                              .collect(Collectors.toList());
            mongoTemplate.insert(operations, submittedOperationCollection);
            clusteringCounts.addSubmittedVariantsUpdateOperationWritten(operations.size());
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
     * From EVA-2071, do not cluster submitted variants into a multimap clustered variant.
     *
     * This function removes candidate clustered variant accessions if they are multimap. This means that some submitted
     * variants will be kept unclustered. This potentially will be revisited in the future, but for now (release 2) we
     * are leaving this out of scope.
     */
    private List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> excludeMultimaps(
            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers) {
        return accessionWrappers.stream().filter(x -> !isMultimap(x.getData())).collect(Collectors.toList());
    }

    /**
     * This function assigns a clustered variant accession (rs) to the submitted variants that didn't have any.
     */
    private void clusterSubmittedVariants(List<? extends SubmittedVariantEntity> submittedVariantEntities) {
        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              SubmittedVariantEntity.class);
        BulkOperations dbsnpBulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                   DbsnpSubmittedVariantEntity.class);
        BulkOperations bulkHistoryOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                SubmittedVariantOperationEntity.class);
        BulkOperations dbsnpBulkHistoryOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                DbsnpSubmittedVariantOperationEntity.class);
        long numUpdates = 0;
        long numDbsnpUpdates = 0;
        for (SubmittedVariantEntity submittedVariantEntity : submittedVariantEntities) {
            if (submittedVariantEntity.getClusteredVariantAccession() != null) {
                // no need to update the rs of a submitted variant that already has the correct rs
                continue;
            }

            Long rsid = getClusteredVariantAccession(submittedVariantEntity);
            if (rsid == null) {
                // no candidate for clustering. e.g. the candidate is a multimap clustered variant (EVA-2071)
                clusteringCounts.addSubmittedVariantsKeptUnclustered(1);
                continue;
            }
            // Query to update the RSid in submittedVariantEntity
            Query updateRsQuery = query(where("_id").is(submittedVariantEntity.getId()));
            Update updateRsUpdate = new Update();
            updateRsUpdate.set(RS_KEY, rsid);

            // Query to create the update operation history
            SubmittedVariantOperationEntity updateOperation = new SubmittedVariantOperationEntity();
            SubmittedVariantInactiveEntity inactiveEntity = new SubmittedVariantInactiveEntity(submittedVariantEntity);
            updateOperation.fill(
                    EventType.UPDATED,
                    submittedVariantEntity.getAccession(),
                    null,
                    "Clustering submitted variant " + submittedVariantEntity.getAccession() + " with rs" + rsid,
                    Collections.singletonList(inactiveEntity)
            );

            if (isEvaSubmittedVariant(submittedVariantEntity)) {
                bulkOperations.updateOne(updateRsQuery, updateRsUpdate);
                bulkHistoryOperations.insert(updateOperation);
                ++numUpdates;
            } else {
                dbsnpBulkOperations.updateOne(updateRsQuery, updateRsUpdate);
                dbsnpBulkHistoryOperations.insert(updateOperation);
                ++numDbsnpUpdates;
            }
        }
        if (numUpdates > 0) {
            bulkOperations.execute();
            clusteringCounts.addSubmittedVariantsClustered(numUpdates);
            bulkHistoryOperations.execute();
            clusteringCounts.addSubmittedVariantsUpdateOperationWritten(numUpdates);
        }
        if (numDbsnpUpdates > 0) {
            dbsnpBulkOperations.execute();
            clusteringCounts.addSubmittedVariantsClustered(numDbsnpUpdates);
            dbsnpBulkHistoryOperations.execute();
            clusteringCounts.addSubmittedVariantsUpdateOperationWritten(numDbsnpUpdates);
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
