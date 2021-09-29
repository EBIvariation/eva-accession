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

import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;

import javax.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class RSMergeWriter implements ItemWriter<SubmittedVariantOperationEntity> {

    private final ClusteringWriter clusteringWriter;

    private final MongoTemplate mongoTemplate;

    private final ClusteringCounts clusteringCounts;

    private static final String ID_ATTRIBUTE = "_id";

    private static final String ACCESSION_ATTRIBUTE = "accession";

    private static final String MERGE_DESTINATION_ATTRIBUTE = "mergeInto";

    public RSMergeWriter(ClusteringWriter clusteringWriter, MongoTemplate mongoTemplate,
                         ClusteringCounts clusteringCounts) {
        this.clusteringWriter = clusteringWriter;
        this.mongoTemplate = mongoTemplate;
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
            clusteringWriter.merge(mergeDestination.getAccession(), mergeDestination.getHashedMessage(),
                                   mergee.getAccession());
            // Record merge operation since the merge method above won't write operations
            // for mergees which don't yet have a record in the clustered variant collection (case during remapping)
            insertMergeOperation(mergeDestination, mergee);
        }
    }

    private void insertMergeOperation(ClusteredVariantEntity mergeDestination, ClusteredVariantEntity mergee) {
        Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                operationsCollectionToWriteTo = clusteringWriter.getClusteredOperationCollection(
                mergee.getAccession());
        List existingOperations =
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
        List existingRecords =
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
}
