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

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
        List<SubmittedVariantEntity> svesToDiscard = new ArrayList<>();

        // Resolve duplicate hashes and accessions before inserting; note that hash resolution must happen first.
        resolveDuplicateHashes(svesToInsert);
        resolveDuplicateAccessions(svesToInsert, svesToDiscard);
        List<SubmittedVariantOperationEntity> discardOperations = getDiscardOperations(svesToDiscard);

        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              SubmittedVariantEntity.class,
                                                              collection);
        if (svesToInsert.size() > 0) {
            bulkOperations.insert(svesToInsert);
        }
        if (svesToDiscard.size() > 0) {
            bulkOperations.remove(query(where("_id").in(
                    svesToDiscard.stream().map(SubmittedVariantEntity::getHashedMessage)
                                 .collect(Collectors.toSet()))));
            BulkOperations svoeBulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                      SubmittedVariantOperationEntity.class,
                                                                      operationCollection);
            svoeBulkOperations.insert(discardOperations);
            svoeBulkOperations.execute();
        }
        if (svesToInsert.size() > 0 || svesToDiscard.size() > 0) {
            BulkWriteResult bulkWriteResult = bulkOperations.execute();
            remappingIngestCounts.addRemappedVariantsIngested(bulkWriteResult.getInsertedCount());
            remappingIngestCounts.addRemappedVariantsDiscarded(bulkWriteResult.getDeletedCount());
        }
    }

    private void resolveDuplicateHashes(List<SubmittedVariantEntity> svesToInsert) {
        Map<String, List<SubmittedVariantEntity>> svesGroupedByHash = getDuplicateHashes(svesToInsert);
        List<SubmittedVariantEntity> svesToNotInsert = new ArrayList<>();

        // This method should not discard anything from DB, only not insert new variants.
        // These will only be logged and will not generate operations.
        List<SubmittedVariantEntity> duplicates = flattenValues(svesGroupedByHash);
        for (SubmittedVariantEntity sve : duplicates) {
            if (svesToInsert.remove(sve)) {
                svesToNotInsert.add(sve);
            }
        }

        if (svesToNotInsert.size() > 0) {
            logger.info("Not inserting due to duplicate hashes: " + svesToNotInsert);
            remappingIngestCounts.addRemappedVariantsDiscarded(svesToNotInsert.size());
        }
    }

    private Map<String, List<SubmittedVariantEntity>> getDuplicateHashes(List<SubmittedVariantEntity> sves) {
        List<String> hashes = sves.stream().map(SubmittedVariantEntity::getHashedMessage).collect(Collectors.toList());

        // Find duplicate hashes already in db
        List<SubmittedVariantEntity> svesWithSameHash = mongoTemplate.find(query(where("_id").in(hashes)),
                                                                           SubmittedVariantEntity.class, collection);

        Map<String, List<SubmittedVariantEntity>> svesGroupedByHash =
                Stream.concat(sves.stream(), svesWithSameHash.stream())
                      .collect(Collectors.groupingBy(SubmittedVariantEntity::getHashedMessage));

        return filterForDuplicates(svesGroupedByHash);
    }

    private void resolveDuplicateAccessions(List<SubmittedVariantEntity> svesToInsert,
                                            List<SubmittedVariantEntity> svesToDiscard) {
        Map<Long, List<SubmittedVariantEntity>> svesGroupedByAccession = getDuplicateAccessions(svesToInsert);
        List<SubmittedVariantEntity> svesToNotInsert = new ArrayList<>();

        List<SubmittedVariantEntity> duplicateSves = flattenValues(svesGroupedByAccession);
        Map<String, LocalDateTime> createdDateByHash = getAllCreatedDateFromSource(duplicateSves);

        for (Map.Entry<Long, List<SubmittedVariantEntity>> ssAndSve: svesGroupedByAccession.entrySet()) {
            List<SubmittedVariantEntity> duplicates = ssAndSve.getValue();
            // Ensure we only keep one out of the list of duplicates for this SS.
            SubmittedVariantEntity currentKept = duplicates.get(0);
            SubmittedVariantDiscardDeterminants currentDeterminants = new SubmittedVariantDiscardDeterminants(
                    currentKept,
                    currentKept.getAccession(),
                    currentKept.getRemappedFrom(),
                    createdDateByHash.get(currentKept.getHashedMessage()));
            for (int i = 1; i < duplicates.size(); i++) {
                SubmittedVariantEntity other = duplicates.get(i);
                DiscardPriority priority = SubmittedVariantDiscardPolicy.prioritise(
                        currentDeterminants,
                        new SubmittedVariantDiscardDeterminants(other,
                                                                other.getAccession(),
                                                                other.getRemappedFrom(),
                                                                createdDateByHash.get(other.getHashedMessage())));
                currentDeterminants = priority.sveToKeep;
                currentKept = priority.sveToKeep.getSve();
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
            remappingIngestCounts.addRemappedVariantsDiscarded(svesToNotInsert.size());
        }
    }

    private Map<Long, List<SubmittedVariantEntity>> getDuplicateAccessions(List<SubmittedVariantEntity> sves) {
        List<Long> accessions = sves.stream().map(SubmittedVariantEntity::getAccession).collect(Collectors.toList());

        // Find duplicate SS already in the database
        List<SubmittedVariantEntity> svesWithSameAccession = mongoTemplate.find(
                query(where("seq").is(assemblyAccession).and("accession").in(accessions)),
                SubmittedVariantEntity.class, collection);

        Map<Long, List<SubmittedVariantEntity>> svesGroupedBySs =
                Stream.concat(sves.stream(), svesWithSameAccession.stream())
                      .collect(Collectors.groupingBy(SubmittedVariantEntity::getAccession));

        return filterForDuplicates(svesGroupedBySs);
    }

    private Map<String, LocalDateTime> getAllCreatedDateFromSource(List<SubmittedVariantEntity> duplicateSves) {
        // By default use the target SVE's created date
        Map<String, LocalDateTime> targetHashToSourceCreatedDate = duplicateSves
                .stream().collect(Collectors.toMap(SubmittedVariantEntity::getHashedMessage,
                                                   SubmittedVariantEntity::getCreatedDate));

        Map<String, List<SubmittedVariantEntity>> svesBySourceAssembly = duplicateSves
                .stream().collect(Collectors.groupingBy(SubmittedVariantEntity::getRemappedFrom));

        for (Map.Entry<String, List<SubmittedVariantEntity>> asmAndSves : svesBySourceAssembly.entrySet()) {
            String sourceAsm = asmAndSves.getKey();
            List<SubmittedVariantEntity> svesRemappedFromAsm = asmAndSves.getValue();

            // If not remapped, stick with the target created date
            if (sourceAsm == null) {
                continue;
            }

            // Otherwise query database for source SVEs with same accession in this assembly to get their created date
            List<Long> targetAccessions = svesRemappedFromAsm
                    .stream()
                    .map(SubmittedVariantEntity::getAccession)
                    .collect(Collectors.toList());
            List<SubmittedVariantEntity> allSourceSvesInAsm = mongoTemplate.find(
                    query(where("seq").is(sourceAsm).and("accession").in(targetAccessions)),
                    SubmittedVariantEntity.class, collection);
            Map<Long, List<SubmittedVariantEntity>> sourceSvesByAccession = allSourceSvesInAsm
                    .stream().collect(Collectors.groupingBy(SubmittedVariantEntity::getAccession));

            for (SubmittedVariantEntity sve : svesRemappedFromAsm) {
                List<SubmittedVariantEntity> sourceSves = sourceSvesByAccession.get(sve.getAccession());
                targetHashToSourceCreatedDate.put(sve.getHashedMessage(), getCreatedDateFromSource(sve, sourceSves));
            }
        }

        return targetHashToSourceCreatedDate;
    }

    private LocalDateTime getCreatedDateFromSource(SubmittedVariantEntity targetSve,
                                                   List<SubmittedVariantEntity> sourceSves) {
        // If we can't find the exact source SVE for any reason, use the created date of the remapped SVE
        if (sourceSves.isEmpty()) {
            logger.warn("No SS " + targetSve.getAccession() + " found in source assembly " + targetSve.getRemappedFrom());
            return targetSve.getCreatedDate();
        }
        if (sourceSves.size() > 1) {
            logger.warn("Duplicate SS " + targetSve.getAccession() + " found in assembly " + targetSve.getRemappedFrom());
            return targetSve.getCreatedDate();
        }
        return sourceSves.get(0).getCreatedDate();
    }

    private List<SubmittedVariantOperationEntity> getDiscardOperations(List<SubmittedVariantEntity> svesToDiscard) {
        return svesToDiscard.stream().map(
                sve -> {
                    SubmittedVariantOperationEntity svoe = new SubmittedVariantOperationEntity();
                    svoe.fill(EventType.DISCARDED,
                              sve.getAccession(),
                              "Submitted variant discarded due to duplicate SS IDs in assembly",
                              Collections.singletonList(new SubmittedVariantInactiveEntity(sve)));
                    return svoe;
                }).collect(Collectors.toList());
    }

    private <T, S>Map<T, List<S>> filterForDuplicates(Map<T, List<S>> map) {
        return map.entrySet().stream()
                  .filter(candidateEntry -> candidateEntry.getValue().size() > 1)
                  .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private <T, S>List<S> flattenValues(Map<T, List<S>> map) {
        return map.values().stream()
                  .flatMap(List::stream)
                  .collect(Collectors.toList());
    }
}
