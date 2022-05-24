/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.clustering.batch.io.SubmittedVariantSplittingPolicy.SplitDeterminants;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetric;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class SSSplitWriter implements ItemWriter<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(SSSplitWriter.class);

    private final String assembly;

    private final ClusteringWriter clusteringWriter;

    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;

    private static final String ID_ATTRIBUTE = "_id";

    private final MongoTemplate mongoTemplate;

    private final List<SubmittedVariantOperationEntity> allSplitCandidatesForCurrentBatch = new ArrayList<>();

    private final MetricCompute<ClusteringMetric> metricCompute;

    public SSSplitWriter(String assembly, ClusteringWriter clusteringWriter,
                         SubmittedVariantAccessioningService submittedVariantAccessioningService,
                         MongoTemplate mongoTemplate, MetricCompute<ClusteringMetric> metricCompute) {
        this.assembly = assembly;
        this.clusteringWriter = clusteringWriter;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
        this.mongoTemplate = mongoTemplate;
        this.metricCompute = metricCompute;
    }

    @Override
    public void write(@Nonnull List<? extends SubmittedVariantEntity> submittedVariantEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException, InstantiationException,
            IllegalAccessException {
        // We do this to avoid tedious generic type signatures in the method calls
        List<SubmittedVariantEntity> svesWithDuplicateID = new ArrayList<>(submittedVariantEntities);

        // All the following steps are designed to be idempotent
        registerSplitCandidates(svesWithDuplicateID);
        processSplitCandidates(this.allSplitCandidatesForCurrentBatch);
        removeSplitCandidates(this.allSplitCandidatesForCurrentBatch);
    }

    protected void processSplitCandidates(List<SubmittedVariantOperationEntity> splitCandidateOperations)
            throws AccessionCouldNotBeGeneratedException {
        Map<String, SubmittedVariantEntity> svesToCreateWithNewIDs = new HashMap<>();
        for (SubmittedVariantOperationEntity operation: splitCandidateOperations) {
            List<SplitDeterminants> splitCandidates =
                    operation.getInactiveObjects()
                             .stream().map(svie -> new SplitDeterminants(svie.toSubmittedVariantEntity(),
                                                                         svie.getHashedMessage(),
                                                                         svie.getClusteredVariantAccession()))
                             .collect(Collectors.toList());
            // Based on the split policy, one of the SS hashes will retain the SS associated with it
            // and the other hashes should be associated with new SS IDs
            List<SubmittedVariantEntity> svesThatShouldGetNewIDs =  getSVEsThatShouldGetNewIDs(splitCandidates);

            for(SplitDeterminants splitCandidate: splitCandidates) {
                if(svesThatShouldGetNewIDs.contains(splitCandidate.getSubmittedVariantEntity())) {
                    svesToCreateWithNewIDs.put(splitCandidate.getSSHash(), splitCandidate.getSubmittedVariantEntity());
                }
            }
        }
        excludeSSWithAlreadyUpdatedIDs(svesToCreateWithNewIDs);
        removeCurrentSSEntriesInDBForSplitCandidates(svesToCreateWithNewIDs.keySet());
        List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> newlyCreatedSVEs =
                this.submittedVariantAccessioningService.getOrCreate(new ArrayList<>(svesToCreateWithNewIDs.values()));
        this.metricCompute.addCount(ClusteringMetric.SUBMITTED_VARIANTS_SS_SPLIT, newlyCreatedSVEs.size());
        recordSplitOperation(svesToCreateWithNewIDs, newlyCreatedSVEs);
    }

    private void excludeSSWithAlreadyUpdatedIDs(Map<String, SubmittedVariantEntity> ssHashesAndAssociatedSS) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> existingSSList =
                this.submittedVariantAccessioningService.get(new ArrayList<>(ssHashesAndAssociatedSS.values()));

        for(AccessionWrapper<ISubmittedVariant, String, Long> existingSSEntryInDB: existingSSList) {
            // If some existing SS in the database have already been provided an updated accession
            // when processing a split operation previously, remove such SS from being considered for split
            String existingSSEntryInDBHash = existingSSEntryInDB.getHash();
            if(ssHashesAndAssociatedSS.containsKey(existingSSEntryInDBHash) &&
                    !existingSSEntryInDB.getAccession().equals(ssHashesAndAssociatedSS
                                                                       .get(existingSSEntryInDBHash).getAccession()))
            {
                ssHashesAndAssociatedSS.remove(existingSSEntryInDB.getHash());
            }
        }
    }

    protected void removeCurrentSSEntriesInDBForSplitCandidates(Set<String> ssHashesToBeGivenNewIDs) {
        Query queryToRemoveCurrentSSEntries = query(where(ID_ATTRIBUTE).in(ssHashesToBeGivenNewIDs));
        this.mongoTemplate.remove(queryToRemoveCurrentSSEntries, SubmittedVariantEntity.class);
        this.mongoTemplate.remove(queryToRemoveCurrentSSEntries, DbsnpSubmittedVariantEntity.class);
    }

    private void recordSplitOperation
            (Map<String, SubmittedVariantEntity> oldSVEs,
             List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> newlyCreatedSVEs) {
        Map<String, EventDocument<ISubmittedVariant, Long,
                ? extends SubmittedVariantInactiveEntity>> svoeOpsToWrite = new HashMap<>();
        Map<String, EventDocument<ISubmittedVariant, Long,
                ? extends SubmittedVariantInactiveEntity>> dbsnpSvoeOpsToWrite = new HashMap<>();

        for(GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long> newlyCreatedSVE: newlyCreatedSVEs) {
            SubmittedVariantEntity oldSVE = oldSVEs.get(newlyCreatedSVE.getHash());
            String idForSplitOperation = String.format("SS_SPLIT_FROM_%s_TO_%s", oldSVE.getAccession(),
                                                       newlyCreatedSVE.getAccession());
            logger.info("Processed split operation " + idForSplitOperation + "...");
            SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
            splitOperation.fill(EventType.SS_SPLIT,
                                oldSVE.getAccession(), newlyCreatedSVE.getAccession(),
                                String.format("SS split from %s to %s", oldSVE.getAccession(),
                                              newlyCreatedSVE.getAccession()) +
                                        " due to same SS ID being assigned to different SS loci",
                                Collections.singletonList(new SubmittedVariantInactiveEntity(oldSVE)));
            splitOperation.setId(idForSplitOperation);
            if (this.clusteringWriter.isEvaSubmittedVariant(oldSVE)) {
                svoeOpsToWrite.put(idForSplitOperation, splitOperation);
            }
            else {
                dbsnpSvoeOpsToWrite.put(idForSplitOperation, splitOperation);
            }
        }

        // Remove entries for which operations have already been recorded
        this.mongoTemplate.find(query(where(ID_ATTRIBUTE).in(svoeOpsToWrite.keySet())),
                                SubmittedVariantOperationEntity.class)
                          .forEach(existingSvoeOp -> svoeOpsToWrite.remove(existingSvoeOp.getId()));
        this.mongoTemplate.find(query(where(ID_ATTRIBUTE).in(dbsnpSvoeOpsToWrite.keySet())),
                                DbsnpSubmittedVariantOperationEntity.class)
                          .forEach(existingDbsnpSvoeOp -> dbsnpSvoeOpsToWrite.remove(existingDbsnpSvoeOp.getId()));

        if (svoeOpsToWrite.values().size() > 0) {
            this.mongoTemplate.insert(svoeOpsToWrite.values(), SubmittedVariantOperationEntity.class);
        }
        if (dbsnpSvoeOpsToWrite.values().size() > 0) {
            this.mongoTemplate.insert(dbsnpSvoeOpsToWrite.values(), DbsnpSubmittedVariantOperationEntity.class);
        }
    }

    protected void removeSplitCandidates(List<SubmittedVariantOperationEntity> splitCandidatesForCurrentBatch) {
        List<String> splitCandidateIDsToFind = splitCandidatesForCurrentBatch.stream().map(
                SubmittedVariantOperationEntity::getId).collect(Collectors.toList());
        this.mongoTemplate.remove(query(where(ID_ATTRIBUTE).in(splitCandidateIDsToFind)),
                                  SubmittedVariantOperationEntity.class);
    }

    protected void registerSplitCandidates(List<SubmittedVariantEntity> svesWithDuplicateID) {
        this.allSplitCandidatesForCurrentBatch.clear();
        // Get a map of split candidate ID -> List of SVEs involved in the split
        Map<String, List<SubmittedVariantEntity>> idSVEMap =
                getIdSVEMapForSplitCandidateOperations(svesWithDuplicateID);

        Set<String> idsForSplitCandidateOperationsToWrite = idSVEMap.keySet();
        Query queryToLookUpExistingOps = query(where(ID_ATTRIBUTE).in(idsForSplitCandidateOperationsToWrite));
        List<SubmittedVariantOperationEntity> existingSplitCandidateOperationsInDB =
                this.mongoTemplate.find(queryToLookUpExistingOps, SubmittedVariantOperationEntity.class);
        this.allSplitCandidatesForCurrentBatch.addAll(existingSplitCandidateOperationsInDB);
        Set<String> existingSplitCandidateOperationIdsInDB =
                existingSplitCandidateOperationsInDB.stream().map(SubmittedVariantOperationEntity::getId)
                                                    .collect(Collectors.toSet());
        // Remove operations that are already in DB
        idsForSplitCandidateOperationsToWrite.removeAll(existingSplitCandidateOperationIdsInDB);
        // Bulk write split candidates for current batch
        writeSplitCandidatesForCurrentBatchToDB(idSVEMap, idsForSplitCandidateOperationsToWrite);
    }

    private Map<String, List<SubmittedVariantEntity>> getIdSVEMapForSplitCandidateOperations(
            List<SubmittedVariantEntity> svesWithDuplicateID) {
        Map<String, List<SubmittedVariantEntity>> idSVEMapForSplitCandidateOperations = new HashMap<>();
        List<Long> duplicateSSIDs = svesWithDuplicateID.stream().map(SubmittedVariantEntity::getAccession)
                                                       .collect(Collectors.toList());

        // The input list that we get may only have one of the SS entries that share duplicate IDs
        // We have to fetch the actual duplicates themselves in order to construct split candidates
        List<SubmittedVariantEntity> svesWithDuplicateIDAlongWithActualDuplicates =
                this.submittedVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(this.assembly, duplicateSSIDs)
                        .stream()
                        .map(result -> new SubmittedVariantEntity(result.getAccession(), result.getHash(),
                                                                  result.getData(), result.getVersion()))
                        .filter(sve -> !this.variantHasMultiMapOrMismatchedAlleles(sve))
                        .collect(Collectors.toList());

        for (SubmittedVariantEntity sve: svesWithDuplicateIDAlongWithActualDuplicates) {
            String idForSplitCandidateOperation = String.format("SS_SPLIT_CANDIDATES_%s_%s",
                                                                sve.getReferenceSequenceAccession(),
                                                                sve.getAccession());
            if (!idSVEMapForSplitCandidateOperations.containsKey(idForSplitCandidateOperation)) {
                idSVEMapForSplitCandidateOperations.put(idForSplitCandidateOperation,
                                                        new ArrayList<>(Collections.singletonList(sve)));
            }
            else {
                idSVEMapForSplitCandidateOperations.get(idForSplitCandidateOperation).add(sve);
            }
        }
        return idSVEMapForSplitCandidateOperations;
    }

    private void writeSplitCandidatesForCurrentBatchToDB(Map<String, List<SubmittedVariantEntity>> idSVEMap,
                                                         Set<String> idsForSplitCandidateOperationsToWrite) {
        // Only new split candidates in the current batch should be written to the database
        List<SubmittedVariantOperationEntity> newSplitCandidatesForCurrentBatch = new ArrayList<>();
        for (String id: idsForSplitCandidateOperationsToWrite) {
            SubmittedVariantOperationEntity splitCandidateOperation = new SubmittedVariantOperationEntity();
            List<SubmittedVariantEntity> svesThatShareSameID = idSVEMap.get(id);
            // Ensure that non-duplicate SS IDs are not processed
            if (svesThatShareSameID.size() > 1) {
                List<SubmittedVariantInactiveEntity> splitCandidates =
                        svesThatShareSameID.stream().map(SubmittedVariantInactiveEntity::new).collect(
                                Collectors.toList());
                splitCandidateOperation.fill(EventType.SS_SPLIT_CANDIDATES,
                                             svesThatShareSameID.get(0).getAccession(),
                                             "Same SS ID assigned to different SS loci",
                                             splitCandidates);
                splitCandidateOperation.setId(id);
                newSplitCandidatesForCurrentBatch.add(splitCandidateOperation);
            }
        }
        if (newSplitCandidatesForCurrentBatch.size() > 0) {
            this.mongoTemplate.insert(newSplitCandidatesForCurrentBatch, SubmittedVariantOperationEntity.class);
            this.allSplitCandidatesForCurrentBatch.addAll(newSplitCandidatesForCurrentBatch);
        }
    }

    private boolean variantHasMultiMapOrMismatchedAlleles(SubmittedVariantEntity sve) {
        return (!sve.isAllelesMatch()) || (Objects.nonNull(sve.getMapWeight()) && sve.getMapWeight() > 1);
    }

    // Get hashes that should receive the new SS IDs
    private List<SubmittedVariantEntity> getSVEsThatShouldGetNewIDs(List<SplitDeterminants> splitCandidates) {
        SplitDeterminants lastPrioritizedHash = splitCandidates.get(0);
        for (int i = 1; i < splitCandidates.size(); i++) {
            lastPrioritizedHash = SubmittedVariantSplittingPolicy.prioritise(
                    lastPrioritizedHash, splitCandidates.get(i)).hashThatShouldRetainOldSS;
        }
        final SplitDeterminants hashThatShouldRetainOldSS = lastPrioritizedHash;
        return splitCandidates.stream()
                .map(SplitDeterminants::getSubmittedVariantEntity)
                .filter(sve -> !(sve.getHashedMessage().equals(hashThatShouldRetainOldSS.getSSHash())))
                .collect(Collectors.toList());
    }
}
