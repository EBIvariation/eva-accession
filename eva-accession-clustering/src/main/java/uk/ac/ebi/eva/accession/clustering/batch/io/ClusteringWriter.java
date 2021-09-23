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
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

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
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
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
import static org.springframework.data.mongodb.core.query.Update.update;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.Priority;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.prioritise;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.getSplitCandidatesQuery;

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

    public static final String ACCESSION_KEY = "accession";

    public static final String REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION = "asm";

    public static final String REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_VARIANT_COLLECTION = "seq";

    public static final String RS_KEY = "rs";

    public static final String INACTIVE_OBJECT_ATTRIBUTE = "inactiveObjects";

    public static final String RS_KEY_IN_OPERATIONS_COLLECTION = INACTIVE_OBJECT_ATTRIBUTE + "." + RS_KEY;

    public static final String ID = "_id";

    private MongoTemplate mongoTemplate;

    private String assemblyAccession;

    private ClusteredVariantAccessioningService clusteredService;

    private SubmittedVariantAccessioningService submittedService;

    private Function<ISubmittedVariant, String> submittedHashingFunction;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    private Map<String, Long> assignedAccessions;

    private Long accessioningMonotonicInitSs;

    private Long accessioningMonotonicInitRs;

    private ClusteringCounts clusteringCounts;

    private boolean processClusteredRemappedVariants;

    public ClusteringWriter(MongoTemplate mongoTemplate,
                            String assemblyAccession,
                            SubmittedVariantAccessioningService submittedVariantAccessioningService,
                            ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                            Long accessioningMonotonicInitSs,
                            Long accessioningMonotonicInitRs,
                            ClusteringCounts clusteringCounts,
                            boolean processClusteredRemappedVariants) {
        this.mongoTemplate = mongoTemplate;
        this.assemblyAccession = assemblyAccession;
        this.submittedService = submittedVariantAccessioningService;
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
    public void write(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException, AccessionDoesNotExistException {
        assignedAccessions.clear();

        // Write new Clustered Variants in mongo and get existing ones. May merge clustered variants
        getOrCreateClusteredVariantAccessions(submittedVariantEntities);

        // Update submitted variants "rs" field
        clusterSubmittedVariants(submittedVariantEntities);
    }

    private void getOrCreateClusteredVariantAccessions(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws AccessionCouldNotBeGeneratedException, AccessionDoesNotExistException {
        if (processClusteredRemappedVariants) {
            Set<SubmittedVariantEntity> processedSubmittedVariants = processClusteredRemappedVariants(submittedVariantEntities);
            submittedVariantEntities = Collections.unmodifiableList(submittedVariantEntities.stream()
                            .filter(sve->!processedSubmittedVariants.contains(sve))
                            .collect(Collectors.toList()));
        }
        List<ClusteredVariant> clusteredVariants = submittedVariantEntities.stream()
                                                                           .map(this::toClusteredVariant)
                                                                           .collect(Collectors.toList());
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
    private Set<SubmittedVariantEntity> processClusteredRemappedVariants(List<? extends SubmittedVariantEntity> submittedVariants) {
        List<SubmittedVariantEntity> clusteredRemappedSubmittedVariants = getClusteredAndRemappedVariants(submittedVariants);
        if(clusteredRemappedSubmittedVariants.isEmpty()){
            return Collections.emptySet();
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

        for (SubmittedVariantEntity submittedVariantEntity : submittedVariants) {
            ClusteredVariantEntity clusteredVariantEntity = toClusteredVariantEntity(submittedVariantEntity);

            boolean isMergeCandidate = checkIfCandidateForMerge(submittedVariantEntity, clusteredVariantEntity,
                    assembly, allExistingHashesInDB, mergeCandidateSVOE);

            updateAllExistingHashesGroupByRS(allExistingHashesGroupByRS, assembly, clusteredVariantEntity.getAccession());

            boolean isSplitCandidate = checkIfCandidateForRSSplit(submittedVariantEntity, clusteredVariantEntity,
                    assembly, allExistingHashesGroupByRS, rsSplitCandidateSVOE);

            if (!isMergeCandidate) {
                if(clusteredVariantEntity.getAccession() >= accessioningMonotonicInitRs){
                    clusteredVariantEntities.add(clusteredVariantEntity);
                }else{
                    dbsnpClusteredVariantEntities.add(clusteredVariantEntity);
                }
                allExistingHashesGroupByRS.get(clusteredVariantEntity.getAccession())
                        .add(clusteredVariantEntity.getHashedMessage());
                allExistingHashesInDB.put(clusteredVariantEntity.getHashedMessage(), clusteredVariantEntity.getAccession());
            }
        }

        insertAllEntriesInDB(clusteredVariantEntities, dbsnpClusteredVariantEntities, mergeCandidateSVOE, rsSplitCandidateSVOE);

        return new HashSet<>(clusteredRemappedSubmittedVariants);
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
        Query querySubmitted = query(where("seq").is(assembly).and("rs").is(accession));
        return mongoTemplate.find(querySubmitted, SubmittedVariantEntity.class);
    }

    private boolean checkIfCandidateForMerge(SubmittedVariantEntity submittedVariantEntity,
                                             ClusteredVariantEntity clusteredVariantEntity,
                                             String assembly,
                                             Map<String, Long> allExistingHashesInDB,
                                             Map<String, SubmittedVariantOperationEntity> mergeCandidateSVOE) {
        Long variantAccession = clusteredVariantEntity.getAccession();
        String variantHash = clusteredVariantEntity.getHashedMessage();
        Long accessionInDB = allExistingHashesInDB.get(variantHash);

        if (allExistingHashesInDB.containsKey(variantHash) && variantAccession != accessionInDB) {
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

    private boolean checkIfCandidateForRSSplit(SubmittedVariantEntity submittedVariantEntity,
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
            return true;
        }

        return false;
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

    private ClusteredVariantEntity toClusteredVariantEntity(SubmittedVariantEntity submittedVariantEntity) {
        return new ClusteredVariantEntity(submittedVariantEntity.getClusteredVariantAccession(),
                getClusteredVariantHash(submittedVariantEntity),
                toClusteredVariant(submittedVariantEntity));
    }

    private ClusteredVariant toClusteredVariant(ISubmittedVariant submittedVariant) {
        ClusteredVariant clusteredVariant = new ClusteredVariant(submittedVariant.getReferenceSequenceAccession(),
                                                                 submittedVariant.getTaxonomyAccession(),
                                                                 submittedVariant.getContig(),
                                                                 submittedVariant.getStart(),
                                                                 getVariantType(
                                                                         submittedVariant.getReferenceAllele(),
                                                                         submittedVariant.getAlternateAllele()),
                                                                 submittedVariant.isValidated(),
                                                                 submittedVariant.getCreatedDate());
        return clusteredVariant;
    }

    private VariantType getVariantType(String reference, String alternate) {
        VariantType variantType = VariantClassifier.getVariantClassification(reference, alternate);
        return variantType;
    }

    private void checkForMerges(List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws AccessionDoesNotExistException {
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

    protected void merge(Long providedAccession, String hash, Long accessionInDatabase)
            throws AccessionDoesNotExistException {
        Priority prioritised = prioritise(providedAccession, accessionInDatabase);

        //Confine merge updates to the particular assembly where clustering is being performed
        Query queryForMergee = query(where(ACCESSION_KEY).is(prioritised.accessionToBeMerged))
                .addCriteria(
                        where(REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION).is(this.assemblyAccession));
        Query queryForMergeTarget = query(where(ACCESSION_KEY).is(prioritised.accessionToKeep))
                .addCriteria(
                        where(REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION).is(this.assemblyAccession));

        List<? extends ClusteredVariantEntity> clusteredVariantToMerge =
                mongoTemplate.find(queryForMergee, getClusteredVariantCollection(prioritised.accessionToBeMerged));

        List<? extends ClusteredVariantEntity> clusteredVariantToKeep =
                mongoTemplate.find(queryForMergeTarget, getClusteredVariantCollection(prioritised.accessionToKeep));

        if (isMultimap(clusteredVariantToMerge) || isMultimap(clusteredVariantToKeep)) {
            // multimap! don't merge. see isMultimap() below for more details
            return;
        }

        if (clusteredVariantToKeep.isEmpty()) {
            throw new AccessionDoesNotExistException("Merge destination RS ID: " + prioritised.accessionToKeep
                                                             + " does not exist!");
        }

        assignedAccessions.put(hash, prioritised.accessionToKeep);

        // write operations for clustered variant being merged
        List<ClusteredVariantOperationEntity> operations =
                clusteredVariantToMerge.stream()
                                       .map(c -> buildClusteredOperation(c, prioritised.accessionToKeep))
                                       .collect(Collectors.toList());
        mongoTemplate.insert(operations, getClusteredOperationCollection(prioritised.accessionToBeMerged));
        clusteringCounts.addClusteredVariantsMergeOperationsWritten(clusteredVariantToMerge.size());

        // Mergee is no longer valid to be present in the main clustered variant collection
        mongoTemplate.remove(queryForMergee, getClusteredVariantCollection(prioritised.accessionToBeMerged));
        clusteringCounts.addClusteredVariantsUpdated(clusteredVariantToMerge.size());

        // Update submitted variants linked to the clustered variant we just merged.
        // This has to happen for both EVA and dbsnp SS because previous cross merges might have happened.
        updateSubmittedVariants(prioritised, SubmittedVariantEntity.class, SubmittedVariantOperationEntity.class);
        updateSubmittedVariants(prioritised, DbsnpSubmittedVariantEntity.class,
                                DbsnpSubmittedVariantOperationEntity.class);

        // Update currently outstanding split candidate events that involve the RS that was just merged and the target RS
        // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1664799060
        updateSplitCandidates(prioritised);
    }

    private void updateSplitCandidates(Priority prioritised) {
        Query queryForSplitCandidatesInvolvingTargetRS = getSplitCandidatesQuery(this.assemblyAccession)
                .addCriteria(where(RS_KEY_IN_OPERATIONS_COLLECTION).is(prioritised.accessionToKeep));
        Query queryForSplitCandidatesInvolvingMergee = getSplitCandidatesQuery(this.assemblyAccession)
                .addCriteria(where(RS_KEY_IN_OPERATIONS_COLLECTION).is(prioritised.accessionToBeMerged));
        // Since the mergee has been merged into the target RS,
        // the split candidates record for mergee are no longer valid - so, delete them!
        mongoTemplate.remove(queryForSplitCandidatesInvolvingMergee, SubmittedVariantOperationEntity.class);

        // There should only be one split candidate record per RS
        SubmittedVariantOperationEntity splitCandidateInvolvingTargetRS =
                mongoTemplate.findOne(queryForSplitCandidatesInvolvingTargetRS, SubmittedVariantOperationEntity.class);

        List<SubmittedVariantInactiveEntity> ssClusteredUnderTargetRS =
                this.submittedService
                        .getByClusteredVariantAccessionIn(Collections.singletonList(prioritised.accessionToKeep))
                        .stream()
                        .map(result -> new SubmittedVariantEntity(result.getAccession(), result.getHash(),
                                                                  result.getData(), result.getVersion()))
                        .map(SubmittedVariantInactiveEntity::new)
                        .collect(Collectors.toList());

        //Update existing split candidates record for target RS if it exists. Else, create a new record!
        if (Objects.nonNull(splitCandidateInvolvingTargetRS)) {
            mongoTemplate.updateFirst(query(where(ID).is(splitCandidateInvolvingTargetRS.getId())),
                                      update(INACTIVE_OBJECT_ATTRIBUTE, ssClusteredUnderTargetRS),
                                      SubmittedVariantOperationEntity.class);
        } else {
            Set<String> targetRSDistinctLoci = ssClusteredUnderTargetRS.stream()
                                                                       .map(entity -> getClusteredVariantHash(
                                                                               entity.getModel()))
                                                                       .collect(Collectors.toSet());
            // Condition for generating split operation: ensure that there is more than one locus sharing the target RS
            if (targetRSDistinctLoci.size() > 1) {
                // Use a convention of lowest SS for testability
                Long lowestSS = ssClusteredUnderTargetRS.stream().map(InactiveSubDocument::getAccession)
                                                        .min(Comparator.naturalOrder()).get();
                SubmittedVariantOperationEntity newSplitCandidateRecord = new SubmittedVariantOperationEntity();
                // TODO: Refactor to use common fill method for split candidates generation
                // to avoid duplicating reason text and call semantics
                newSplitCandidateRecord.fill(EventType.RS_SPLIT_CANDIDATES, lowestSS,
                                             "Hash mismatch with " + prioritised.accessionToKeep,
                                             ssClusteredUnderTargetRS);
                mongoTemplate.insert(Collections.singletonList(newSplitCandidateRecord),
                                     SubmittedVariantOperationEntity.class);
            }
        }
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
    private boolean isMultimap(List<? extends IClusteredVariant> clusteredVariants) {
        return clusteredVariants.stream().anyMatch(cv -> cv.getMapWeight() != null && cv.getMapWeight() > 1);
    }

    private boolean isMultimap(IClusteredVariant clusteredVariant) {
        return isMultimap(Collections.singletonList(clusteredVariant));
    }

    protected Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
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
        Query querySubmitted = query(where(RS_KEY).is(prioritised.accessionToBeMerged))
                .addCriteria(
                        where(REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_VARIANT_COLLECTION).is(this.assemblyAccession));
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

    protected boolean isEvaSubmittedVariant(SubmittedVariantEntity submittedVariant) {
        return submittedVariant.getAccession() >= accessioningMonotonicInitSs;
    }

    private Long getClusteredVariantAccession(SubmittedVariantEntity submittedVariantEntity) {
        String hash = getClusteredVariantHash(submittedVariantEntity);
        return assignedAccessions.get(hash);
    }

    private String getClusteredVariantHash(ISubmittedVariant submittedVariant) {
        ClusteredVariant clusteredVariant = toClusteredVariant(submittedVariant);
        String hash = clusteredHashingFunction.apply(clusteredVariant);
        return hash;
    }
}
