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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.mongodb.MongoBulkWriteException;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetric;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import javax.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.getMergeCandidatesCriteria;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.getSplitCandidatesCriteria;

public class RSMergeWriter implements ItemWriter<SubmittedVariantOperationEntity> {

    private static final Logger logger = LoggerFactory.getLogger(RSMergeWriter.class);

    private final ClusteringWriter clusteringWriter;

    private final MongoTemplate mongoTemplate;

    private final String assemblyAccession;

    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;

    private final MetricCompute metricCompute;

    private static final String ID_ATTRIBUTE = "_id";

    private static final String ACCESSION_ATTRIBUTE = "accession";

    private static final String MERGE_DESTINATION_ATTRIBUTE = "mergeInto";

    private static final String RS_KEY = "rs";

    private static final String INACTIVE_OBJECT_ATTRIBUTE = "inactiveObjects";

    private static final String RS_KEY_IN_OPERATIONS_COLLECTION = INACTIVE_OBJECT_ATTRIBUTE + "." + RS_KEY;

    private static final String REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION = "asm";

    private static final String REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_VARIANT_COLLECTION = "seq";

    private static final String ASM_ATTRIBUTE_IN_OPERATIONS_COLLECTION = INACTIVE_OBJECT_ATTRIBUTE + "." +
            REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION;

    private int currentlyProcessingOperationIndex;

    private List<SubmittedVariantOperationEntity> allMergeCandidateOperations;

    private static class OperationWithIndex {
        int operationIndex;
        SubmittedVariantOperationEntity operation;

        public OperationWithIndex(int operationIndex, SubmittedVariantOperationEntity operation) {
            this.operationIndex = operationIndex;
            this.operation = operation;
        }
    }

    private Map<Long, List<OperationWithIndex>> rsIDIndexedOperations;

    public RSMergeWriter(ClusteringWriter clusteringWriter, MongoTemplate mongoTemplate,
                         String assemblyAccession,
                         SubmittedVariantAccessioningService submittedVariantAccessioningService,
                         MetricCompute metricCompute) {
        this.clusteringWriter = clusteringWriter;
        this.mongoTemplate = mongoTemplate;
        this.assemblyAccession = assemblyAccession;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
        this.metricCompute = metricCompute;
    }

    @Override
    public void write(@Nonnull List<? extends SubmittedVariantOperationEntity> submittedVariantOperationEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException, AccessionDoesNotExistException {
        allMergeCandidateOperations = new ArrayList<>(submittedVariantOperationEntities);
        // Create a map of all merge candidate operations keyed by RS ID to facilitate quick random lookups
        populateOperationsIndex(allMergeCandidateOperations);
        for (int i = 0; i < allMergeCandidateOperations.size(); i++) {
            this.currentlyProcessingOperationIndex = i;
            writeRSMerge(allMergeCandidateOperations.get(i));
        }
        List<String> allCandidateIds = allMergeCandidateOperations.stream().map(EventDocument::getId)
                                                                  .collect(Collectors.toList());
        this.mongoTemplate.findAllAndRemove(query(where("_id").in(allCandidateIds)),
                                            SubmittedVariantOperationEntity.class);
    }

    private void populateOperationsIndex
            (List<? extends SubmittedVariantOperationEntity> allMergeCandidateOperations) {
        rsIDIndexedOperations = new HashMap<>();
        for (int i = 0; i < allMergeCandidateOperations.size(); i++) {
            SubmittedVariantOperationEntity operation = allMergeCandidateOperations.get(i);
            for (Long rsID:
                    operation.getInactiveObjects().stream()
                             .map(SubmittedVariantInactiveEntity::getClusteredVariantAccession)
                             .collect(Collectors.toSet())) {
                OperationWithIndex obj = new OperationWithIndex(i, operation);
                if (rsIDIndexedOperations.containsKey(rsID)) {
                    rsIDIndexedOperations.get(rsID).add(obj);
                } else {
                    rsIDIndexedOperations.put(rsID, new ArrayList<>(Arrays.asList(obj)));
                }
            }
        }
    }

    // Get distinct objects based on an object attribute
    // https://stackoverflow.com/a/27872852
    private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }

    public void writeRSMerge(SubmittedVariantOperationEntity currentOperation)
            throws AccessionDoesNotExistException {
        // Given a merge candidate event with many SS: https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=267493761
        // get the corresponding RS involved - involves de-duplication using distinctByKey for RS accessions
        // since multiple SS might have the same RS
        // Note that we cannot just use distinct after the map() below for de-duplication
        // because ClusteredVariantEntity "equals" method does NOT involve comparing accessions
        List<ClusteredVariantEntity> mergeCandidates =
                currentOperation.getInactiveObjects()
                                .stream()
                                // Ensure duplicates inside inactiveObjects are tolerated
                                .filter(distinctByKey(this::getHashedMessageAndAccessionForSVIE))
                                .map(entity -> clusteringWriter.toClusteredVariantEntity(
                                        entity.toSubmittedVariantEntity()))
                                .filter(distinctByKey(AccessionedDocument::getAccession))
                                .collect(Collectors.toList());
        // From among the participating RS in a merge,
        // use the current RS prioritization policy to get the target RS into which the rest of the RS will be merged
        ImmutablePair<ClusteredVariantEntity, List<ClusteredVariantEntity>> mergeDestinationAndMergees =
                getMergeDestinationAndMergees(mergeCandidates);
        ClusteredVariantEntity mergeDestination = mergeDestinationAndMergees.getLeft();
        List<ClusteredVariantEntity> mergees = mergeDestinationAndMergees.getRight();

        removeMergeesAndInsertMergeDestination(mergeDestination, mergees);

        for (ClusteredVariantEntity mergee: mergees) {
            logger.info("RS merge operation: Merging rs{} to rs{} due to hash collision...",
                        mergee.getAccession(), mergeDestination.getAccession());
            recordMergeOperations(mergeDestination, mergee, currentOperation);
        }
    }

    private void removeMergeesAndInsertMergeDestination(ClusteredVariantEntity mergeDestination,
                                                        List<ClusteredVariantEntity> mergeeList) {
        Query queryForCheckingExistingCVE = query(where(ID_ATTRIBUTE).is(mergeDestination.getHashedMessage()));
        List<ClusteredVariantEntity> existingCVEList = mongoTemplate.find(queryForCheckingExistingCVE, ClusteredVariantEntity.class);
        List<DbsnpClusteredVariantEntity> existingDbsnpCVEList = mongoTemplate.find(queryForCheckingExistingCVE, DbsnpClusteredVariantEntity.class);
        ClusteredVariantEntity existingCVE = null;
        if (existingCVEList != null && !existingCVEList.isEmpty()) {
            existingCVE = existingCVEList.get(0);
        }
        if (existingDbsnpCVEList != null && !existingDbsnpCVEList.isEmpty()) {
            existingCVE = existingDbsnpCVEList.get(0);
        }

        if (existingCVE == null) {
            insertRSRecordForMergeDestination(mergeDestination);
        } else {
            if (!existingCVE.getAccession().equals(mergeDestination.getAccession())) {
                mongoTemplate.remove(query(where(ID_ATTRIBUTE).is(existingCVE.getHashedMessage())),
                        clusteringWriter.getClusteredVariantCollection(existingCVE.getAccession()));
                metricCompute.addCount(ClusteringMetric.CLUSTERED_VARIANTS_UPDATED, 1);
                insertRSRecordForMergeDestination(mergeDestination);
            }
        }
    }

    private ImmutablePair<String, Long> getHashedMessageAndAccessionForSVIE(SubmittedVariantInactiveEntity svie) {
        return new ImmutablePair<>(svie.getHashedMessage(), svie.getAccession());
    }

    private void insertMergeOperation(ClusteredVariantEntity mergeDestination, ClusteredVariantEntity mergee) {
        Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                operationsCollectionToWriteTo = clusteringWriter.getClusteredOperationCollection(mergee.getAccession());
        List<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                existingOperations =
                this.mongoTemplate.find(query(where(ACCESSION_ATTRIBUTE).is(mergee.getAccession()))
                                                .addCriteria(where(MERGE_DESTINATION_ATTRIBUTE).is(
                                                        mergeDestination.getAccession()))
                                                .addCriteria(where(ASM_ATTRIBUTE_IN_OPERATIONS_COLLECTION)
                                                                     .is(this.assemblyAccession)),
                                        operationsCollectionToWriteTo);
        if (existingOperations.isEmpty()) {
            ClusteredVariantOperationEntity operation = new ClusteredVariantOperationEntity();
            operation.fill(EventType.MERGED, mergee.getAccession(), mergeDestination.getAccession(),
                           "After remapping to " + mergee.getAssemblyAccession() +
                                   ", RS IDs mapped to the same locus.",
                           Collections.singletonList(new ClusteredVariantInactiveEntity(mergee)));
            this.mongoTemplate.insert(operation,
                    this.mongoTemplate.getCollectionName(operationsCollectionToWriteTo));
            metricCompute.addCount(ClusteringMetric.CLUSTERED_VARIANTS_MERGE_OPERATIONS, 1);
        }
    }

    private void insertRSRecordForMergeDestination(ClusteredVariantEntity mergeDestination) {
        Class<? extends ClusteredVariantEntity> clusteredVariantCollectionToWriteTo =
                clusteringWriter.getClusteredVariantCollection(mergeDestination.getAccession());
        this.mongoTemplate.insert(mergeDestination,
                this.mongoTemplate.getCollectionName(clusteredVariantCollectionToWriteTo));
        metricCompute.addCount(ClusteringMetric.CLUSTERED_VARIANTS_CREATED, 1);
    }

    /**
     * Get destination RS and the set of RS to be merged into the destination RS
     * @param mergeCandidates Set of RS candidates that should be merged
     * @return Pair with the first element being the destination RS
     * and the next being the list of RS that should be merged into the former.
     */
    private ImmutablePair<ClusteredVariantEntity, List<ClusteredVariantEntity>> getMergeDestinationAndMergees(
            List<? extends ClusteredVariantEntity> mergeCandidates) {
        Long lastPrioritizedAccession = mergeCandidates.get(0).getAccession();
        for (int i = 1; i < mergeCandidates.size(); i++) {
            lastPrioritizedAccession = ClusteredVariantMergingPolicy.prioritise(
                    lastPrioritizedAccession, mergeCandidates.get(i).getAccession()).accessionToKeep;
        }
        final Long targetRSAccession = lastPrioritizedAccession;
        ClusteredVariantEntity targetRS = mergeCandidates.stream().filter(rs -> rs.getAccession()
                                                                                  .equals(targetRSAccession))
                                                         .findFirst().get();
        List<ClusteredVariantEntity> mergees = mergeCandidates.stream()
                                                              .filter(rs -> !rs.getAccession()
                                                                               .equals(targetRSAccession))
                                                              .collect(Collectors.toList());
        return new ImmutablePair<>(targetRS, mergees);
    }

    protected void recordMergeOperations(ClusteredVariantEntity mergeDestination, ClusteredVariantEntity mergee,
                         SubmittedVariantOperationEntity currentOperation) {
        Long accessionToBeMerged = mergee.getAccession();
        Long accessionToKeep = mergeDestination.getAccession();

        // Record merge operation
        insertMergeOperation(mergeDestination, mergee);

        // Update submitted variants linked to the clustered variant we just merged.
        // This has to happen for both EVA and dbsnp SS because previous cross merges might have happened.
        ClusteredVariantMergingPolicy.Priority prioritised =
                new ClusteredVariantMergingPolicy.Priority(accessionToKeep, accessionToBeMerged);
        updateSubmittedVariants(prioritised, currentOperation, SubmittedVariantEntity.class,
                                SubmittedVariantOperationEntity.class);
        updateSubmittedVariants(prioritised, currentOperation, DbsnpSubmittedVariantEntity.class,
                                DbsnpSubmittedVariantOperationEntity.class);

        // Update other merge candidate operations involving the mergee
        // by replacing references to mergee RS ID with the target RS ID
        // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1454412665
        updateMergeCandidatesInvolvingMergee(prioritised, currentOperation);

        // Update currently outstanding split candidate events that involve the RS that was just merged and the target RS
        // See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=1664799060
        updateSplitCandidates(prioritised);
    }

    private void updateMergeCandidatesInvolvingMergee(ClusteredVariantMergingPolicy.Priority prioritised,
                                                      SubmittedVariantOperationEntity currentMergeOperation) {
        updateOperationsInDB(prioritised, currentMergeOperation);
        // Update the upcoming in-memory objects in this batch as well
        updateOperationsInMemory(prioritised, currentMergeOperation);
    }

    private void updateOperationsInDB(ClusteredVariantMergingPolicy.Priority prioritised,
                                      SubmittedVariantOperationEntity currentMergeOperation) {
        Query queryForMergeCandidatesInvolvingMergee = query(new Criteria().andOperator(
                getMergeCandidatesCriteria(this.assemblyAccession),
                where(ID_ATTRIBUTE).ne(currentMergeOperation.getId()),
                where(RS_KEY_IN_OPERATIONS_COLLECTION).is(prioritised.accessionToBeMerged)));
        List<SubmittedVariantOperationEntity> operationsInDBInvolvingMergee =
                mongoTemplate.find(queryForMergeCandidatesInvolvingMergee, SubmittedVariantOperationEntity.class);
        for (SubmittedVariantOperationEntity operation: operationsInDBInvolvingMergee) {
            List<SubmittedVariantInactiveEntity> submittedVariantInactiveEntitiesWithMergeeRSReplaced =
                    operation.getInactiveObjects().stream()
                             .map(entity -> replaceRSInSubmittedVariantInactiveEntity(
                                     entity, prioritised.accessionToBeMerged, prioritised.accessionToKeep)
                             ).collect(Collectors.toList());
            mongoTemplate.updateFirst(query(where(ID_ATTRIBUTE).is(operation.getId())),
                                      update(INACTIVE_OBJECT_ATTRIBUTE,
                                             submittedVariantInactiveEntitiesWithMergeeRSReplaced),
                                      SubmittedVariantOperationEntity.class);
        }
    }

    private void updateOperationsInMemory(ClusteredVariantMergingPolicy.Priority prioritised,
                           SubmittedVariantOperationEntity currentMergeOperation) {
        if (rsIDIndexedOperations.containsKey(prioritised.accessionToBeMerged)) {
            List<OperationWithIndex> operationsInMemoryInvolvingMergee =
                    rsIDIndexedOperations.get(prioritised.accessionToBeMerged)
                                         .stream()
                                         // Filter for upcoming operations only
                                         .filter(e -> (e.operationIndex > this.currentlyProcessingOperationIndex)
                                                 && !(e.operation.equals(currentMergeOperation)))
                                         .collect(Collectors.toList());
            for (OperationWithIndex operationWithIndex: operationsInMemoryInvolvingMergee) {
                SubmittedVariantOperationEntity operation = operationWithIndex.operation;
                List<SubmittedVariantInactiveEntity> inactiveEntities =
                        operation.getInactiveObjects().stream()
                                 .map(entity -> replaceRSInSubmittedVariantInactiveEntity
                                         (entity, prioritised.accessionToBeMerged, prioritised.accessionToKeep))
                                 .collect(Collectors.toList());
                SubmittedVariantOperationEntity updatedOperation =
                        new SubmittedVariantOperationEntity();
                updatedOperation.fill(operation.getEventType(), operation.getAccession(), operation.getReason(),
                        inactiveEntities);
                updatedOperation.setId(operation.getId());
                allMergeCandidateOperations.set(operationWithIndex.operationIndex, updatedOperation);
            }
        }
    }

    private SubmittedVariantInactiveEntity replaceRSInSubmittedVariantInactiveEntity(
            SubmittedVariantInactiveEntity inactiveEntity, Long rsAccessionToReplace, Long replacementRSAccession) {
        if (inactiveEntity.getClusteredVariantAccession().equals(rsAccessionToReplace)) {
            SubmittedVariantEntity temp = inactiveEntity.toSubmittedVariantEntity();
            temp.setClusteredVariantAccession(replacementRSAccession);
            return new SubmittedVariantInactiveEntity(temp);
        }
        return inactiveEntity;
    }

    private void updateSplitCandidates(ClusteredVariantMergingPolicy.Priority prioritised) {
        Query queryForSplitCandidatesInvolvingTargetRS = query(getSplitCandidatesCriteria(this.assemblyAccession)
                                                                       .and(RS_KEY_IN_OPERATIONS_COLLECTION)
                                                                       .is(prioritised.accessionToKeep));
        Query queryForSplitCandidatesInvolvingMergee = query(getSplitCandidatesCriteria(this.assemblyAccession)
                                                                     .and(RS_KEY_IN_OPERATIONS_COLLECTION)
                                                                     .is(prioritised.accessionToBeMerged));
        // Since the mergee has been merged into the target RS,
        // the split candidates record for mergee are no longer valid - so, delete them!
        mongoTemplate.remove(queryForSplitCandidatesInvolvingMergee, SubmittedVariantOperationEntity.class);

        // There should only be one split candidate record per RS
        SubmittedVariantOperationEntity splitCandidateInvolvingTargetRS =
                mongoTemplate.findOne(queryForSplitCandidatesInvolvingTargetRS, SubmittedVariantOperationEntity.class);

        List<SubmittedVariantInactiveEntity> ssClusteredUnderTargetRS =
                this.submittedVariantAccessioningService
                        .getByClusteredVariantAccessionIn(Collections.singletonList(prioritised.accessionToKeep),
                                                          ContigNamingConvention.NO_REPLACEMENT)
                        .stream()
                        .filter(result -> result.getData().getReferenceSequenceAccession()
                                                .equals(this.assemblyAccession))
                        .map(result -> new SubmittedVariantEntity(result.getAccession(), result.getHash(),
                                                                  result.getData(), result.getVersion()))
                        .map(SubmittedVariantInactiveEntity::new)
                        .collect(Collectors.toList());
        Map<String, List<ClusteredVariantEntity>> distinctLociInDBForTargetRS =
                ssClusteredUnderTargetRS.stream()
                                .map(SubmittedVariantInactiveEntity::toSubmittedVariantEntity)
                                .map(clusteringWriter::toClusteredVariantEntity)
                                .collect(Collectors.groupingBy(ClusteredVariantEntity::getHashedMessage));

        //Update existing split candidates record for target RS if it exists. Else, create a new record!
        if (Objects.nonNull(splitCandidateInvolvingTargetRS) && distinctLociInDBForTargetRS.size() > 1) {
            mongoTemplate.updateFirst(query(where(ID_ATTRIBUTE).is(splitCandidateInvolvingTargetRS.getId())),
                                      update(INACTIVE_OBJECT_ATTRIBUTE, ssClusteredUnderTargetRS),
                                      SubmittedVariantOperationEntity.class);
        } else {
            Set<String> targetRSDistinctLoci =
                    ssClusteredUnderTargetRS.stream().map(entity -> clusteringWriter
                                                    .getClusteredVariantHash(entity.getModel()))
                                            .collect(Collectors.toSet());
            // Condition for generating split operation: ensure that there is more than one locus sharing the target RS
            if (targetRSDistinctLoci.size() > 1) {
                // The new SPLIT candidate is for rs prioritised.accessionToKeep
                SubmittedVariantOperationEntity newSplitCandidateRecord = new SubmittedVariantOperationEntity();
                // TODO: Refactor to use common fill method for split candidates generation
                // to avoid duplicating reason text and call semantics
                newSplitCandidateRecord.fill(EventType.RS_SPLIT_CANDIDATES, prioritised.accessionToKeep,
                        "Hash mismatch with " + prioritised.accessionToKeep,
                        ssClusteredUnderTargetRS);
                newSplitCandidateRecord.setId(ClusteringWriter.getSplitCandidateId(newSplitCandidateRecord));
                mongoTemplate.insert(Collections.singletonList(newSplitCandidateRecord),
                        SubmittedVariantOperationEntity.class);
                metricCompute.addCount(ClusteringMetric.CLUSTERED_VARIANTS_RS_SPLIT, 1);
            }
        }
    }

    /**
     * This function updates the clustered variant accession (rs) of submitted variants when the rs makes a
     * collision with another rs and they have to be merged.
     */
    private void updateSubmittedVariants(
            ClusteredVariantMergingPolicy.Priority prioritised,
            SubmittedVariantOperationEntity ssParticipatingInMerge,
            Class<? extends SubmittedVariantEntity> submittedVariantCollection,
            Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                    submittedOperationCollection) {
        Query querySubmitted = query(where(RS_KEY).is(prioritised.accessionToBeMerged))
                .addCriteria(
                        where(REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_VARIANT_COLLECTION).is(this.assemblyAccession));
        List<? extends SubmittedVariantEntity> svToUpdate =
                mongoTemplate.find(querySubmitted, submittedVariantCollection);
        List<String> svToUpdateIds = svToUpdate.stream().map(AccessionedDocument::getId).collect(Collectors.toList());

        Update update = new Update();
        update.set(RS_KEY, prioritised.accessionToKeep);
        mongoTemplate.updateMulti(query(where("_id").in(svToUpdateIds)), update, submittedVariantCollection);
        metricCompute.addCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATED_RS, svToUpdate.size());

        List<SubmittedVariantOperationEntity> operations =
                svToUpdate.stream()
                          .map(sv -> buildSubmittedOperation(sv, prioritised.accessionToKeep))
                          .collect(Collectors.toList());
        mongoTemplate.insert(operations, submittedOperationCollection);
        metricCompute.addCount(ClusteringMetric.SUBMITTED_VARIANTS_UPDATE_OPERATIONS, operations.size());
    }

    private SubmittedVariantOperationEntity buildSubmittedOperation(SubmittedVariantEntity originalSubmittedVariant,
                                                                    Long clusteredVariantMergedInto) {
        SubmittedVariantInactiveEntity inactiveEntity = new SubmittedVariantInactiveEntity(originalSubmittedVariant);

        Long originalClusteredVariant = originalSubmittedVariant.getClusteredVariantAccession();
        String reason = "Original rs" + originalClusteredVariant + " associated with SS was merged into rs"
                + clusteredVariantMergedInto + ".";

        Long accession = originalSubmittedVariant.getAccession();
        SubmittedVariantOperationEntity operation = new SubmittedVariantOperationEntity();

        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // anything. We are updating the submitted variant, changing its rs field
        operation.fill(EventType.UPDATED, accession, null, reason, Collections.singletonList(inactiveEntity));
        return operation;
    }
}
