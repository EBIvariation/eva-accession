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
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;

import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.InactiveSubDocument;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static org.springframework.data.mongodb.core.query.Update.update;
import static uk.ac.ebi.eva.accession.clustering.batch.io.ClusteredVariantMergingPolicy.prioritise;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.getSplitCandidatesQuery;

public class RSMergeWriter implements ItemWriter<SubmittedVariantOperationEntity> {

    private final ClusteringWriter clusteringWriter;

    private final MongoTemplate mongoTemplate;

    private final String assemblyAccession;

    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;

    private final ClusteringCounts clusteringCounts;

    private static final String ID_ATTRIBUTE = "_id";

    private static final String ACCESSION_ATTRIBUTE = "accession";

    private static final String MERGE_DESTINATION_ATTRIBUTE = "mergeInto";

    private static final String RS_KEY = "rs";

    private static final String INACTIVE_OBJECT_ATTRIBUTE = "inactiveObjects";

    private static final String RS_KEY_IN_OPERATIONS_COLLECTION = INACTIVE_OBJECT_ATTRIBUTE + "." + RS_KEY;

    private static final String REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION = "asm";

    private static final String REFERENCE_ASSEMBLY_FIELD_IN_SUBMITTED_VARIANT_COLLECTION = "seq";

    public RSMergeWriter(ClusteringWriter clusteringWriter, MongoTemplate mongoTemplate,
                         String assemblyAccession,
                         SubmittedVariantAccessioningService submittedVariantAccessioningService,
                         ClusteringCounts clusteringCounts) {
        this.clusteringWriter = clusteringWriter;
        this.mongoTemplate = mongoTemplate;
        this.assemblyAccession = assemblyAccession;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
        this.clusteringCounts = clusteringCounts;
    }

    @Override
    public void write(@Nonnull List<? extends SubmittedVariantOperationEntity> submittedVariantOperationEntities)
            throws MongoBulkWriteException, AccessionCouldNotBeGeneratedException, AccessionDoesNotExistException {
        for (SubmittedVariantOperationEntity entity: submittedVariantOperationEntities) {
            writeRSMerge(entity);
            this.mongoTemplate.remove(entity,
                                      this.mongoTemplate.getCollectionName(SubmittedVariantOperationEntity.class));
        }
    }

    // Get distinct objects based on an object attribute
    // https://stackoverflow.com/a/27872852
    private static <T> Predicate<T> distinctByKey(Function<? super T, ?> keyExtractor) {
        Set<Object> seen = ConcurrentHashMap.newKeySet();
        return t -> seen.add(keyExtractor.apply(t));
    }

    public void writeRSMerge(SubmittedVariantOperationEntity submittedVariantOperationEntity)
            throws AccessionDoesNotExistException {
        // Given a merge candidate event with many SS: https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=267493761
        // get the corresponding RS involved - involves de-duplication using distinctByKey for RS accessions
        // since multiple SS might have the same RS
        // Note that we cannot just use distinct after the map() below for de-duplication
        // because ClusteredVariantEntity "equals" method does NOT involve comparing accessions
        List<ClusteredVariantEntity> mergeCandidates =
                submittedVariantOperationEntity.getInactiveObjects()
                                               .stream()
                                               .map(entity -> clusteringWriter.toClusteredVariantEntity(
                                                       entity.toSubmittedVariantEntity()))
                                               .filter(distinctByKey(AccessionedDocument::getAccession))
                                               .collect(Collectors.toList());
        // From among the participating RS in a merge,
        // use the current RS prioritization policy to get the target RS into which the rest of the RS will be merged
        ImmutablePair<ClusteredVariantEntity, List<ClusteredVariantEntity>> mergeDestinationAndMergees =
                getMergeDestinationAndMergees(mergeCandidates);
        ClusteredVariantEntity mergeDestination = mergeDestinationAndMergees.getLeft();
        // Insert RS record for destination RS if it does not already exist
        insertRSRecordForMergeDestination(mergeDestination);

        List<ClusteredVariantEntity> mergees = mergeDestinationAndMergees.getRight();
        for (ClusteredVariantEntity mergee: mergees) {
            merge(mergeDestination.getAccession(), mergee.getAccession());
            // Record merge operation
            insertMergeOperation(mergeDestination, mergee);
        }
    }

    private void insertMergeOperation(ClusteredVariantEntity mergeDestination, ClusteredVariantEntity mergee) {
        Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                operationsCollectionToWriteTo = clusteringWriter.getClusteredOperationCollection(mergee.getAccession());
        List<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                existingOperations =
                this.mongoTemplate.find(query(where(ACCESSION_ATTRIBUTE).is(mergee.getAccession()))
                                                .addCriteria(where(MERGE_DESTINATION_ATTRIBUTE)
                                                                     .is(mergeDestination.getAccession())),
                                        operationsCollectionToWriteTo);
        if (existingOperations.isEmpty()) {
            ClusteredVariantOperationEntity operation = new ClusteredVariantOperationEntity();
            operation.fill(EventType.MERGED, mergee.getAccession(), mergeDestination.getAccession(),
                           "After remapping to " + mergee.getAssemblyAccession() +
                                   ", RS IDs mapped to the same locus.",
                           Collections.singletonList(new ClusteredVariantInactiveEntity(mergee)));
            this.mongoTemplate.insert(operation,
                                      this.mongoTemplate.getCollectionName(operationsCollectionToWriteTo));
            this.clusteringCounts.addClusteredVariantsMergeOperationsWritten(1);
        }
    }

    private void insertRSRecordForMergeDestination(ClusteredVariantEntity mergeDestination) {
        Class<? extends ClusteredVariantEntity> clusteredVariantCollectionToWriteTo =
                clusteringWriter.getClusteredVariantCollection(mergeDestination.getAccession());
        List<? extends ClusteredVariantEntity> existingRecords =
                this.mongoTemplate.find(query(where(ID_ATTRIBUTE).is(mergeDestination.getHashedMessage()))
                                                .addCriteria(where(ACCESSION_ATTRIBUTE).is(
                                                        mergeDestination.getAccession())),
                                        clusteredVariantCollectionToWriteTo);
        if (existingRecords.isEmpty()) {
            this.mongoTemplate.insert(mergeDestination,
                                      this.mongoTemplate.getCollectionName(clusteredVariantCollectionToWriteTo));
            this.clusteringCounts.addClusteredVariantsCreated(1);
        }
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

    protected void merge(Long providedAccession, Long accessionInDatabase)
            throws AccessionDoesNotExistException {
        ClusteredVariantMergingPolicy.Priority prioritised = prioritise(providedAccession, accessionInDatabase);

        //Confine merge updates to the particular assembly where clustering is being performed
        Query queryForMergee = query(where(ACCESSION_ATTRIBUTE).is(prioritised.accessionToBeMerged))
                .addCriteria(
                        where(REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION).is(this.assemblyAccession));
        Query queryForMergeTarget = query(where(ACCESSION_ATTRIBUTE).is(prioritised.accessionToKeep))
                .addCriteria(
                        where(REFERENCE_ASSEMBLY_FIELD_IN_CLUSTERED_VARIANT_COLLECTION).is(this.assemblyAccession));

        List<? extends ClusteredVariantEntity> clusteredVariantToMerge =
                mongoTemplate.find(queryForMergee, clusteringWriter.getClusteredVariantCollection(
                        prioritised.accessionToBeMerged));

        List<? extends ClusteredVariantEntity> clusteredVariantToKeep =
                mongoTemplate.find(queryForMergeTarget, clusteringWriter.getClusteredVariantCollection(
                        prioritised.accessionToKeep));

        if (clusteringWriter.isMultimap(clusteredVariantToMerge) || clusteringWriter.isMultimap(clusteredVariantToKeep)) {
            // multimap! don't merge. see isMultimap() below for more details
            return;
        }

        if (clusteredVariantToKeep.isEmpty()) {
            throw new AccessionDoesNotExistException("Merge destination RS ID: " + prioritised.accessionToKeep
                                                             + " does not exist!");
        }

        // Mergee is no longer valid to be present in the main clustered variant collection
        mongoTemplate.remove(queryForMergee, clusteringWriter.getClusteredVariantCollection(
                prioritised.accessionToBeMerged));
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

    private void updateSplitCandidates(ClusteredVariantMergingPolicy.Priority prioritised) {
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
                this.submittedVariantAccessioningService
                        .getByClusteredVariantAccessionIn(Collections.singletonList(prioritised.accessionToKeep))
                        .stream()
                        .map(result -> new SubmittedVariantEntity(result.getAccession(), result.getHash(),
                                                                  result.getData(), result.getVersion()))
                        .map(SubmittedVariantInactiveEntity::new)
                        .collect(Collectors.toList());

        //Update existing split candidates record for target RS if it exists. Else, create a new record!
        if (Objects.nonNull(splitCandidateInvolvingTargetRS)) {
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
                this.clusteringCounts.addClusteredVariantsRSSplit(1);
            }
        }
    }

    /**
     * This function updates the clustered variant accession (rs) of submitted variants when the rs makes a
     * collision with another rs and they have to be merged.
     */
    private void updateSubmittedVariants(
            ClusteredVariantMergingPolicy.Priority prioritised,
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
