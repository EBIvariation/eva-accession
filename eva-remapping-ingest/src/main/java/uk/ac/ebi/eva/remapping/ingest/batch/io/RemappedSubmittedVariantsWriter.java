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
package uk.ac.ebi.eva.remapping.ingest.batch.io;

import com.mongodb.MongoBulkWriteException;
import com.mongodb.bulk.BulkWriteResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.exceptions.MongoBulkWriteExceptionUtils;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.remapping.ingest.batch.io.SubmittedVariantDiscardPolicy.DiscardPriority;
import uk.ac.ebi.eva.remapping.ingest.batch.io.SubmittedVariantDiscardPolicy.SubmittedVariantDiscardDeterminants;
import uk.ac.ebi.eva.remapping.ingest.batch.listeners.RemappingIngestCounts;
import uk.ac.ebi.eva.remapping.ingest.configuration.CollectionNames;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class RemappedSubmittedVariantsWriter implements ItemWriter<SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(RemappedSubmittedVariantsWriter.class);

    private MongoTemplate mongoTemplate;

    private String assemblyAccession;

    private String collection;

    private String operationCollection;

    private RemappingIngestCounts remappingIngestCounts;

    public RemappedSubmittedVariantsWriter(MongoTemplate mongoTemplate, String assemblyAccession, String collection,
                                           RemappingIngestCounts remappingIngestCounts) {
        this.mongoTemplate = mongoTemplate;
        this.assemblyAccession = assemblyAccession;
        this.collection = collection;
        this.operationCollection = collection.equals(CollectionNames.SUBMITTED_VARIANT_ENTITY) ?
                CollectionNames.SUBMITTED_VARIANT_OPERATION_ENTITY : CollectionNames.DBSNP_SUBMITTED_VARIANT_OPERATION_ENTITY;
        this.remappingIngestCounts = remappingIngestCounts;
    }

    @Override
    public void write(List<? extends SubmittedVariantEntity> submittedVariantsRemapped) {
        // We do this to avoid tedious generic type signatures in the method calls
        List<SubmittedVariantEntity> svesToInsert = new ArrayList<>(submittedVariantsRemapped);

        try {
            // Resolve potential duplicate SS IDs
            Map<Long, List<SubmittedVariantEntity>> duplicateCandidates = getDuplicateSs(svesToInsert);
            List<SubmittedVariantEntity> svesToDiscard = resolveDuplicates(duplicateCandidates, svesToInsert);
            List<SubmittedVariantOperationEntity> discardOperations = getDiscardOperations(svesToDiscard);

            BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                  SubmittedVariantEntity.class,
                                                                  collection);
            BulkOperations svoeBulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                      SubmittedVariantEntity.class,
                                                                      operationCollection);

            // TODO think about ordering of these statements & idempotency
            bulkOperations.insert(svesToInsert);
            if (svesToDiscard.size() > 0) {
                bulkOperations.remove(query(where("_id").in(
                        svesToDiscard.stream().map(SubmittedVariantEntity::getHashedMessage)
                                     .collect(Collectors.toSet()))));
                svoeBulkOperations.insert(discardOperations);
            }
            BulkWriteResult bulkWriteResult = bulkOperations.execute();
            BulkWriteResult operationWriteResult = svoeBulkOperations.execute();

            // TODO counts.... ?
            remappingIngestCounts.addRemappedVariantsIngested(bulkWriteResult.getInsertedCount());
        } catch (DuplicateKeyException exception) {
            MongoBulkWriteException writeException = ((MongoBulkWriteException) exception.getCause());
            BulkWriteResult bulkWriteResult = writeException.getWriteResult();

            // Log the actual submitted variants we failed to insert due to DuplicateKeyException
            Set<String> duplicateHashesSkipped = MongoBulkWriteExceptionUtils
                    .extractUniqueHashesForDuplicateKeyError(writeException).collect(Collectors.toSet());
            List<SubmittedVariantEntity> duplicateSvesSkipped = svesToInsert
                    .stream()
                    .filter(sve -> duplicateHashesSkipped.contains(sve.getHashedMessage()))
                    .collect(Collectors.toList());
            logger.info("Failed to insert due to duplicate key exception: " + duplicateSvesSkipped);

            int ingested = bulkWriteResult.getInsertedCount();
            remappingIngestCounts.addRemappedVariantsIngested(ingested);
            remappingIngestCounts.addRemappedVariantsSkipped(duplicateSvesSkipped.size());
        }
    }

    private Map<Long, List<SubmittedVariantEntity>> getDuplicateSs(List<SubmittedVariantEntity> sves) {
        Map<Long, List<SubmittedVariantEntity>> svesGroupedBySs = new HashMap<>();

        // Collect potential duplicates from the current batch
        addToSsMap(sves, svesGroupedBySs);

        // Find duplicate SS already in the database
        List<SubmittedVariantEntity> svesWithSameAccession = mongoTemplate.find(
                query(where("seq").is(assemblyAccession).and("accession").in(svesGroupedBySs.keySet())),
                SubmittedVariantEntity.class, collection);
        if (!svesWithSameAccession.isEmpty()) {
            addToSsMap(svesWithSameAccession, svesGroupedBySs);
        }

        return svesGroupedBySs
                .entrySet()
                .stream()
                .filter(candidateEntry -> candidateEntry.getValue().size() > 1)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void addToSsMap(List<SubmittedVariantEntity> sves, Map<Long, List<SubmittedVariantEntity>> ssMap) {
        for (SubmittedVariantEntity sve : sves) {
            ssMap.putIfAbsent(sve.getAccession(), Collections.emptyList());
            ssMap.get(sve.getAccession()).add(sve);
        }
    }

    private List<SubmittedVariantEntity> resolveDuplicates(
            Map<Long, List<SubmittedVariantEntity>> svesGroupedBySs, List<SubmittedVariantEntity> svesToInsert) {
        List<SubmittedVariantEntity> svesToDiscard = new ArrayList<>();
        List<SubmittedVariantEntity> svesToNotInsert = new ArrayList<>();

        // TODO provision for same hash - need to keep the one in DB already if present, to ensure idempotency
        //  and also not creating superfluous discard operations.

        for (Map.Entry<Long, List<SubmittedVariantEntity>> ssAndSve: svesGroupedBySs.entrySet()) {
            List<SubmittedVariantEntity> duplicates = ssAndSve.getValue();
            // Ensure we only keep one out of the list of duplicates for this SS.
            SubmittedVariantEntity currentKept = duplicates.get(0);
            for (int i = 1; i < duplicates.size(); i++) {
                SubmittedVariantEntity other = duplicates.get(i);
                DiscardPriority priority = SubmittedVariantDiscardPolicy.prioritise(
                        new SubmittedVariantDiscardDeterminants(currentKept,
                                                                currentKept.getAccession(),
                                                                currentKept.getRemappedFrom(),
                                                                currentKept.getCreatedDate()),  // TODO this needs to be the created date in the source assembly!!
                        new SubmittedVariantDiscardDeterminants(other,
                                                                other.getAccession(),
                                                                other.getRemappedFrom(),
                                                                other.getCreatedDate()));
                currentKept = priority.sveToKeep;
            }
            // Discard everything that's not currentKept
            for (SubmittedVariantEntity sve : duplicates) {
                if (sve.equals(currentKept)) continue;
                // If the SVE to discard isn't one we were trying to insert, add it to the list of SVEs to remove from
                // the database; these will also generate a discard operation.
                // Otherwise just remove it from the list to insert; these will only be logged.
                if (!svesToInsert.remove(sve)) {
                    svesToDiscard.add(sve);
                } else {
                    svesToNotInsert.add(sve);
                }
            }

        }
        if (svesToNotInsert.size() > 0) {
            logger.info("Not inserting because SS ID already exists in assembly: " + svesToNotInsert);
        }
        return svesToDiscard;
    }

    private List<SubmittedVariantOperationEntity> getDiscardOperations(List<SubmittedVariantEntity> svesToDiscard) {
        return svesToDiscard.stream().map(
                sve -> {
                    SubmittedVariantOperationEntity svoe = new SubmittedVariantOperationEntity();
                    svoe.fill(EventType.DEPRECATED,  // TODO new event type!
                              sve.getAccession(),
                              "Submitted variant discarded due to duplicate SS IDs in assembly",
                              Collections.singletonList(new SubmittedVariantInactiveEntity(sve)));
                    return svoe;
                }).collect(Collectors.toList());
    }
}
