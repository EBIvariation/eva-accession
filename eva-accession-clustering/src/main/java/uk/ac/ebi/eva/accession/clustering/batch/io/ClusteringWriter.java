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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

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
    private static final Logger logger = LoggerFactory.getLogger(ClusteringWriter.class);

    private static final String RS_KEY = "rs";

    private final MongoTemplate mongoTemplate;

    private final ClusteredVariantAccessioningService clusteredService;

    private final Function<ISubmittedVariant, String> submittedHashingFunction;

    private final Function<IClusteredVariant, String> clusteredHashingFunction;

    private final Map<String, Long> assignedAccessions;

    private final Long accessioningMonotonicInitSs;

    private final Long accessioningMonotonicInitRs;

    private final ClusteringCounts clusteringCounts;

    private final boolean processClusteredRemappedVariants;

    public ClusteringWriter(MongoTemplate mongoTemplate,
                            ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                            Long accessioningMonotonicInitSs,
                            Long accessioningMonotonicInitRs,
                            ClusteringCounts clusteringCounts,
                            boolean processClusteredRemappedVariants) {
        this.mongoTemplate = mongoTemplate;
        this.submittedHashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        this.clusteredService = clusteredVariantAccessioningService;
        this.clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        this.assignedAccessions = new HashMap<>();
        Assert.notNull(accessioningMonotonicInitSs, "accessioningMonotonicInitSs must not be null. Check autowiring.");
        this.accessioningMonotonicInitSs = accessioningMonotonicInitSs;
        this.accessioningMonotonicInitRs = accessioningMonotonicInitRs;
        this.clusteringCounts = clusteringCounts;
        this.processClusteredRemappedVariants = processClusteredRemappedVariants;
    }

    @Override
    public void write(@Nonnull List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException, AccessionDoesNotExistException {
        assignedAccessions.clear();

        // Write new Clustered Variants in mongo and get existing ones. May merge clustered variants
        getOrCreateClusteredVariantAccessions(submittedVariantEntities);

        // Update submitted variants "rs" field for unclustered variants
        if (!this.processClusteredRemappedVariants) {
            clusterSubmittedVariants(submittedVariantEntities);
        }
    }

    private void getOrCreateClusteredVariantAccessions(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws AccessionCouldNotBeGeneratedException {
        if (processClusteredRemappedVariants) {
            processClusteredRemappedVariants(submittedVariantEntities);
        } else {
            List<ClusteredVariant> clusteredVariants = submittedVariantEntities.stream()
                                                                               .map(this::toClusteredVariant)
                                                                               .collect(Collectors.toList());
            if (!clusteredVariants.isEmpty()) {
                List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionWrappers =
                        clusteredService.getOrCreate(clusteredVariants);

                List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionNoMultimap =
                        excludeMultimaps(accessionWrappers);

                accessionNoMultimap.forEach(x -> assignedAccessions.put(x.getHash(), x.getAccession()));

                long newAccessions = accessionWrappers.stream().filter(GetOrCreateAccessionWrapper::isNewAccession)
                                                      .count();
                clusteringCounts.addClusteredVariantsCreated(newAccessions);
            }
        }
    }

    /**
     * This method is for the detection of Merge and RS split candidates.
     * These are already clustered variants, but due to remapping, the variants with same rs id might end up in two
     * different locations or different types. These needs to be identified for rectifying later.
     * <p>
     * --------------------Before Clustering--------------------
     *
     *                  SubmittedVariantEntity
     * SS	RS	ASM	    STUDY	CONTIG	POS	    REF	    ALT
     * 500	306	ASM1	PRJEB1	Chr1	1000	A	    T  (original)
     * 501	306	ASM1	PRJEB2	Chr1	1000	A	    T  (original)
     * 500	306	ASM2	PRJEB1	Chr1	1000	A	    T  (remapped)
     * 501	306	ASM2	PRJEB2	Chr1	1500	A	    T   (remapped)
     *
     *                  ClusteredVariantEntity
     * RS	HASH                ASM	    POS	    CONTIG	TYPE
     * 306  ASM1_Chr1_1000_SNV	ASM1	1000	Chr1	SNV
     *
     * SS id 500 and 501 has same RS because of remapping, but they are now at different positions and can't have same RS id.
     * These needs to be identified and stored in submittedVariantOperationEntity table for rectification.
     *
     * --------------------After Detection--------------------
     *
     *                      SubmittedVariantOperationEntity
     * ACCESSION    EVENT_TYPE          REASON                  INACTIVe_OBJECTS
     * 306          RS_SPLIT_CANDIDATE  Hash mismatch with 306  {ss-500 and ss-501}
     */
    private void processClusteredRemappedVariants(List<? extends SubmittedVariantEntity> submittedVariants) {
        List<SubmittedVariantEntity> clusteredRemappedSubmittedVariants = getClusteredAndRemappedVariants(submittedVariants);
        if(clusteredRemappedSubmittedVariants.isEmpty()){
            return;
        }

        String assembly = clusteredRemappedSubmittedVariants.get(0).getReferenceSequenceAccession();
        Map<String, Long> allExistingHashesInDB = getSubmittedVariantsAllExistingHashesInDB(clusteredRemappedSubmittedVariants);
        Map<String, SubmittedVariantOperationEntity> mergeCandidateSVOE = new HashMap<>();
        Map<Long, SubmittedVariantOperationEntity> rsSplitCandidateSVOE = new HashMap<>();
        for(SubmittedVariantOperationEntity svoe: getSVOEWithMergeAndRSSplitCandidates(assembly)){
            if (svoe.getEventType().equals(EventType.RS_MERGE_CANDIDATES)) {
                mergeCandidateSVOE.put(getClusteredVariantHash(svoe.getInactiveObjects().get(0).getModel()), svoe);
            } else if (svoe.getEventType().equals(EventType.RS_SPLIT_CANDIDATES)) {
                rsSplitCandidateSVOE.put(svoe.getAccession(), svoe);
            }
        }

        Map<Long, Set<String>> allExistingHashesGroupByRS = new HashMap<>();
        List<ClusteredVariantEntity> clusteredVariantEntities = new ArrayList<>();
        List<ClusteredVariantEntity> dbsnpClusteredVariantEntities = new ArrayList<>();

        for (SubmittedVariantEntity remappedSubmittedVariantEntity : clusteredRemappedSubmittedVariants) {
            ClusteredVariantEntity clusteredVariantEntity = toClusteredVariantEntity(remappedSubmittedVariantEntity);

            boolean isMergeCandidate = checkIfCandidateForMerge(remappedSubmittedVariantEntity, clusteredVariantEntity,
                                                                assembly, allExistingHashesInDB, mergeCandidateSVOE);

            updateAllExistingHashesGroupByRS(allExistingHashesGroupByRS, assembly,
                                             clusteredVariantEntity.getAccession());

            checkIfCandidateForRSSplit(remappedSubmittedVariantEntity, clusteredVariantEntity, assembly,
                                       allExistingHashesGroupByRS, rsSplitCandidateSVOE);

            if (!isMergeCandidate) {
                if (clusteredVariantEntity.getAccession() >= accessioningMonotonicInitRs) {
                    clusteredVariantEntities.add(clusteredVariantEntity);
                } else {
                    dbsnpClusteredVariantEntities.add(clusteredVariantEntity);
                }
                allExistingHashesGroupByRS.get(clusteredVariantEntity.getAccession())
                                          .add(clusteredVariantEntity.getHashedMessage());
                allExistingHashesInDB.put(clusteredVariantEntity.getHashedMessage(),
                                          clusteredVariantEntity.getAccession());
            }
        }

        insertAllEntriesInDB(clusteredVariantEntities, dbsnpClusteredVariantEntities, mergeCandidateSVOE, rsSplitCandidateSVOE);
    }

    private List<SubmittedVariantEntity> getClusteredAndRemappedVariants(List<? extends SubmittedVariantEntity> submittedVariants) {
        return submittedVariants.stream()
                .filter(sve -> Objects.nonNull(sve.getClusteredVariantAccession()))
                .filter(sve -> !StringUtil.isBlank(sve.getRemappedFrom()))
                .collect(Collectors.toList());
    }

    private Map<String, Long> getSubmittedVariantsAllExistingHashesInDB(List<SubmittedVariantEntity> submittedVariantEntities) {
        List<ClusteredVariant> clusteredVariants = submittedVariantEntities.stream()
                .map(this::toClusteredVariant)
                .collect(Collectors.toList());
        return clusteredService.get(clusteredVariants).stream()
                .collect(Collectors.toMap(AccessionWrapper::getHash, AccessionWrapper::getAccession));
    }

    private Set<String> getAllHashesForAssemblyAndRS(String assembly, Long accession) {
        try {
            return clusteredService.getAllByAccession(accession).stream()
                    .filter(accWrapper -> accWrapper.getData().getAssemblyAccession().equals(assembly))
                    .map(AccessionWrapper::getHash)
                    .collect(Collectors.toSet());
        } catch (AccessionMergedException | AccessionDoesNotExistException | AccessionDeprecatedException ex) {
            logger.error("exception occurred while getting variants with accession ID {}", accession, ex);
        }
        return Collections.emptySet();
    }

    private List<SubmittedVariantOperationEntity> getSVOEWithMergeAndRSSplitCandidates(String assembly) {
        List<String> MERGE_AND_SPLIT_EVENTS = Arrays.asList(EventType.RS_MERGE_CANDIDATES.name(),
                EventType.RS_SPLIT_CANDIDATES.name());
        Query queryOperations = query(where("eventType").in(MERGE_AND_SPLIT_EVENTS)
                .and("inactiveObjects").elemMatch(where("seq").is(assembly)));
        return mongoTemplate.find(queryOperations, SubmittedVariantOperationEntity.class);
    }

    private List<SubmittedVariantEntity> getAllSubmittedVariantsWithClusteringAccession(String assembly, Long accession) {
        List<SubmittedVariantEntity> results = new ArrayList<>();
        Query querySubmitted = query(where("seq").is(assembly).and("rs").is(accession));
        results.addAll(mongoTemplate.find(querySubmitted, SubmittedVariantEntity.class));
        results.addAll(mongoTemplate.find(querySubmitted, DbsnpSubmittedVariantEntity.class));
        return results;
    }

    private boolean checkIfCandidateForMerge(SubmittedVariantEntity submittedVariantEntity,
                                             ClusteredVariantEntity clusteredVariantEntity,
                                             String assembly,
                                             Map<String, Long> allExistingHashesInDB,
                                             Map<String, SubmittedVariantOperationEntity> mergeCandidateSVOE) {
        Long variantAccession = clusteredVariantEntity.getAccession();
        String variantHash = clusteredVariantEntity.getHashedMessage();
        Long accessionInDB = allExistingHashesInDB.get(variantHash);

        if (allExistingHashesInDB.containsKey(variantHash) && !Objects.equals(variantAccession, accessionInDB)) {
            if (mergeCandidateSVOE.containsKey(variantHash)) {
                mergeCandidateSVOE.get(variantHash).getInactiveObjects()
                        .add(new SubmittedVariantInactiveEntity(submittedVariantEntity));
            } else {
                List<SubmittedVariantInactiveEntity> inactiveObjects =
                        getAllSubmittedVariantsWithClusteringAccession(assembly, accessionInDB).stream()
                                .filter(sve-> toClusteredVariantEntity(sve).getHashedMessage().equals(variantHash))
                                .map(SubmittedVariantInactiveEntity::new)
                                .collect(Collectors.toList());
                inactiveObjects.add(new SubmittedVariantInactiveEntity(submittedVariantEntity));
                SubmittedVariantOperationEntity submittedVariantOperationEntity = new SubmittedVariantOperationEntity();
                submittedVariantOperationEntity.fill(EventType.RS_MERGE_CANDIDATES, accessionInDB,
                        "RS mismatch with " + accessionInDB, inactiveObjects);

                mergeCandidateSVOE.put(variantHash, submittedVariantOperationEntity);
            }
            return true;
        }
        return false;
    }

    private void checkIfCandidateForRSSplit(SubmittedVariantEntity submittedVariantEntity,
                                               ClusteredVariantEntity clusteredVariantEntity,
                                               String assembly,
                                               Map<Long, Set<String>> allExistingHashesGroupByRS,
                                               Map<Long, SubmittedVariantOperationEntity> rsSplitCandidateSVOE) {
        Long variantAccession = clusteredVariantEntity.getAccession();
        String variantHash = clusteredVariantEntity.getHashedMessage();

        if (!allExistingHashesGroupByRS.get(variantAccession).isEmpty() &&
                !allExistingHashesGroupByRS.get(variantAccession).contains(variantHash)) {
            if (rsSplitCandidateSVOE.containsKey(variantAccession)) {
                List<SubmittedVariantInactiveEntity> inactiveEntities = rsSplitCandidateSVOE.get(variantAccession)
                        .getInactiveObjects();
                boolean submittedVariantAlreadyExist = inactiveEntities.stream()
                        .anyMatch(sv -> submittedHashingFunction.apply(sv).equals(
                                submittedVariantEntity.getHashedMessage()) &&
                                sv.getAccession().equals(submittedVariantEntity.getAccession()));
                if (!submittedVariantAlreadyExist) {
                    inactiveEntities.add(new SubmittedVariantInactiveEntity(submittedVariantEntity));
                }
            } else {
                SubmittedVariantOperationEntity submittedVariantOperationEntity = new SubmittedVariantOperationEntity();
                List<SubmittedVariantInactiveEntity> inactiveEntities =
                        getAllSubmittedVariantsWithClusteringAccession(assembly, variantAccession).stream()
                                .map(SubmittedVariantInactiveEntity::new)
                                .collect(Collectors.toList());
                submittedVariantOperationEntity.fill(EventType.RS_SPLIT_CANDIDATES, variantAccession,
                        "Hash mismatch with " + variantAccession, inactiveEntities);

                rsSplitCandidateSVOE.put(variantAccession, submittedVariantOperationEntity);
            }
        }
    }

    private void updateAllExistingHashesGroupByRS(Map<Long, Set<String>> allExistingHashesGroupByRS, String assembly,
                                                  Long variantAccession) {
        if (!allExistingHashesGroupByRS.containsKey(variantAccession)) {
            Set<String> allHashForRS = getAllHashesForAssemblyAndRS(assembly, variantAccession);
            if(allHashForRS.isEmpty()) {
                allExistingHashesGroupByRS.put(variantAccession, new HashSet<>());
            }else{
                allExistingHashesGroupByRS.put(variantAccession, allHashForRS);
            }
        }
    }

    private void insertAllEntriesInDB(List<ClusteredVariantEntity> clusteredVariantEntities,
                                      List<ClusteredVariantEntity> dbsnpClusteredVariantEntities,
                                      Map<String, SubmittedVariantOperationEntity> mergeSVOE,
                                      Map<Long, SubmittedVariantOperationEntity> rsSplitSVOE) {
        mongoTemplate.insert(clusteredVariantEntities, ClusteredVariantEntity.class);
        mongoTemplate.insert(dbsnpClusteredVariantEntities, DbsnpClusteredVariantEntity.class);
        this.clusteringCounts.addClusteredVariantsCreated(clusteredVariantEntities.size() +
                                                                  dbsnpClusteredVariantEntities.size());

        List<SubmittedVariantOperationEntity> mergeSVOEInsertEntries = new ArrayList<>();
        List<SubmittedVariantOperationEntity> rsSplitSVOEInsertEntries = new ArrayList<>();

        for (Map.Entry<String, SubmittedVariantOperationEntity> entry : mergeSVOE.entrySet()) {
            SubmittedVariantOperationEntity svoe = entry.getValue();
            if(Objects.isNull(svoe.getId())){
                mergeSVOEInsertEntries.add(svoe);
                continue;
            }
            Query querySubmitted = query(where("_id").is(svoe.getId())
                    .and("eventType").is(EventType.RS_MERGE_CANDIDATES)
                    .and("accession").is(svoe.getAccession())
                    .and("reason").is("RS mismatch with " + svoe.getAccession()));
            Update update = new Update();
            update.set("inactiveObjects", svoe.getInactiveObjects());
            // Since we are updating one specific record in SVOE, update first will be sufficient
            mongoTemplate.updateFirst(querySubmitted, update, SubmittedVariantOperationEntity.class);
        }

        for (Map.Entry<Long, SubmittedVariantOperationEntity> entry : rsSplitSVOE.entrySet()) {
            Long accession = entry.getKey();
            SubmittedVariantOperationEntity svoe = entry.getValue();
            if(Objects.isNull(svoe.getId())){
                rsSplitSVOEInsertEntries.add(svoe);
                continue;
            }
            Query querySubmitted = query(where("_id").is(svoe.getId())
                    .and("eventType").is(EventType.RS_SPLIT_CANDIDATES)
                    .and("accession").is(accession)
                    .and("reason").is("Hash mismatch with " + accession));
            Update update = new Update();
            update.set("inactiveObjects", svoe.getInactiveObjects());
            // Since we are updating one specific record in SVOE, update first will be sufficient
            mongoTemplate.updateFirst(querySubmitted, update, SubmittedVariantOperationEntity.class);
        }

        mongoTemplate.insert(mergeSVOEInsertEntries, SubmittedVariantOperationEntity.class);
        mongoTemplate.insert(rsSplitSVOEInsertEntries, SubmittedVariantOperationEntity.class);
    }

    protected ClusteredVariantEntity toClusteredVariantEntity(SubmittedVariantEntity submittedVariantEntity) {
        return new ClusteredVariantEntity(submittedVariantEntity.getClusteredVariantAccession(),
                getClusteredVariantHash(submittedVariantEntity),
                toClusteredVariant(submittedVariantEntity));
    }

    private ClusteredVariant toClusteredVariant(ISubmittedVariant submittedVariant) {
        return new ClusteredVariant(submittedVariant.getReferenceSequenceAccession(),
                                    submittedVariant.getTaxonomyAccession(),
                                    submittedVariant.getContig(),
                                    submittedVariant.getStart(),
                                    VariantClassifier.getVariantClassification(submittedVariant.getReferenceAllele(),
                                                                               submittedVariant.getAlternateAllele()),
                                    submittedVariant.isValidated(),
                                    submittedVariant.getCreatedDate());
    }

    protected Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
    getClusteredOperationCollection(Long accession) {
        return isEvaClusteredAccession(accession) ?
                ClusteredVariantOperationEntity.class : DbsnpClusteredVariantOperationEntity.class;
    }

    protected Class<? extends ClusteredVariantEntity> getClusteredVariantCollection(Long accession) {
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
    protected boolean isMultimap(List<? extends IClusteredVariant> clusteredVariants) {
        return clusteredVariants.stream().anyMatch(cv -> cv.getMapWeight() != null && cv.getMapWeight() > 1);
    }

    protected boolean isMultimap(IClusteredVariant clusteredVariant) {
        return isMultimap(Collections.singletonList(clusteredVariant));
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

    protected boolean isEvaSubmittedVariant(SubmittedVariantEntity submittedVariant) {
        return submittedVariant.getAccession() >= accessioningMonotonicInitSs;
    }

    private Long getClusteredVariantAccession(SubmittedVariantEntity submittedVariantEntity) {
        String hash = getClusteredVariantHash(submittedVariantEntity);
        return assignedAccessions.get(hash);
    }

    protected String getClusteredVariantHash(ISubmittedVariant submittedVariant) {
        ClusteredVariant clusteredVariant = toClusteredVariant(submittedVariant);
        return clusteredHashingFunction.apply(clusteredVariant);
    }
}
