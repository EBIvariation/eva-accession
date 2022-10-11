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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.ImmutableTriple;
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
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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

        // Resolve duplicate hashes and accessions before inserting; note that hash resolution must happen first.
        ImmutablePair<List<SubmittedVariantEntity>, List<SubmittedVariantOperationEntity>> duplicateHashDiscards =
                resolveDuplicateHashes(svesToInsert);
        ImmutablePair<List<SubmittedVariantEntity>, List<SubmittedVariantOperationEntity>> duplicateAccessionDiscards =
                resolveDuplicateAccessions(svesToInsert);

        List<SubmittedVariantEntity> svesToDiscard = Stream.concat(duplicateHashDiscards.getLeft().stream(),
                                                                    duplicateAccessionDiscards.getLeft().stream())
                                                            .collect(Collectors.toList());
        List<SubmittedVariantOperationEntity> discardOperations = Stream.concat(duplicateHashDiscards.getRight().stream(),
                                                                                duplicateAccessionDiscards.getRight().stream())
                                                                        .collect(Collectors.toList());

        BulkOperations bulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                              SubmittedVariantEntity.class,
                                                              collection);
        // Deal with discards before inserts, to avoid DuplicateKeyExceptions
        if (discardOperations.size() > 0) {
            try {
                BulkOperations svoeBulkOperations = mongoTemplate.bulkOps(BulkOperations.BulkMode.UNORDERED,
                                                                          SubmittedVariantOperationEntity.class,
                                                                          operationCollection);
                svoeBulkOperations.insert(discardOperations);
                BulkWriteResult bulkWriteResult = svoeBulkOperations.execute();
                remappingIngestCounts.addRemappedVariantsDiscarded(bulkWriteResult.getInsertedCount());

            } catch (DuplicateKeyException exception) {
                // As we check for hash collisions in submitted variants, we should only get duplicate keys from
                // trying to insert an identical DISCARD operation, which should only happen when resuming or rerunning.
                MongoBulkWriteException writeException = ((MongoBulkWriteException) exception.getCause());
                BulkWriteResult bulkWriteResult = writeException.getWriteResult();

                List<String> duplicatesSkipped = MongoBulkWriteExceptionUtils
                        .extractUniqueHashesForDuplicateKeyError(writeException).collect(Collectors.toList());
                logger.warn("Duplicate key exception when inserting DISCARD operations: " + duplicatesSkipped);

                remappingIngestCounts.addRemappedVariantsDiscarded(bulkWriteResult.getInsertedCount());
                remappingIngestCounts.addRemappedVariantsSkipped(duplicatesSkipped.size());
            }
        }
        if (svesToDiscard.size() > 0) {
            bulkOperations.remove(query(where("_id").in(
                    svesToDiscard.stream().map(SubmittedVariantEntity::getHashedMessage)
                                 .collect(Collectors.toSet()))));
            bulkOperations.execute();
        }

        if (svesToInsert.size() > 0) {
            bulkOperations.insert(svesToInsert);
            BulkWriteResult bulkWriteResult = bulkOperations.execute();
            remappingIngestCounts.addRemappedVariantsIngested(bulkWriteResult.getInsertedCount());
        }
    }

    private ImmutablePair<List<SubmittedVariantEntity>, List<SubmittedVariantOperationEntity>> resolveDuplicateHashes(
            List<SubmittedVariantEntity> svesToInsert) {
        Map<String, List<SubmittedVariantEntity>> svesGroupedByHash = getDuplicateHashes(svesToInsert);
        return resolveDuplicates(svesToInsert, svesGroupedByHash,
                                 "Submitted variant discarded due to duplicate hash");
    }

    private ImmutablePair<List<SubmittedVariantEntity>, List<SubmittedVariantOperationEntity>> resolveDuplicateAccessions(
            List<SubmittedVariantEntity> svesToInsert) {
        Map<Long, List<SubmittedVariantEntity>> svesGroupedByAccession = getDuplicateAccessions(svesToInsert);
        return resolveDuplicates(svesToInsert, svesGroupedByAccession,
                                 "Submitted variant discarded due to duplicate SS IDs in assembly");
    }

    private <T>ImmutablePair<List<SubmittedVariantEntity>, List<SubmittedVariantOperationEntity>> resolveDuplicates(
            List<SubmittedVariantEntity> svesToInsert,
            Map<T, List<SubmittedVariantEntity>> svesGroupedByKey,
            String discardMessage) {
        List<SubmittedVariantEntity> svesToDiscard = new ArrayList<>();
        List<SubmittedVariantOperationEntity> discardOperations = new ArrayList<>();

        List<SubmittedVariantEntity> duplicateSves = flattenValues(svesGroupedByKey);
        Map<ImmutableTriple<String, Long, String>, LocalDateTime> createdDateMap =
                getAllCreatedDateFromSource(duplicateSves);

        for (Map.Entry<T, List<SubmittedVariantEntity>> keyAndSve: svesGroupedByKey.entrySet()) {
            List<SubmittedVariantEntity> duplicates = keyAndSve.getValue();
            // Ensure we only keep one out of the list of duplicates for this SS.
            SubmittedVariantEntity currentKept = duplicates.get(0);
            SubmittedVariantDiscardDeterminants currentDeterminants = new SubmittedVariantDiscardDeterminants(
                    currentKept,
                    currentKept.getAccession(),
                    currentKept.getRemappedFrom(),
                    createdDateMap.get(getKeyForCreatedDate(currentKept)));
            for (int i = 1; i < duplicates.size(); i++) {
                SubmittedVariantEntity other = duplicates.get(i);
                SubmittedVariantDiscardDeterminants otherDeterminants = new SubmittedVariantDiscardDeterminants(
                        other,
                        other.getAccession(),
                        other.getRemappedFrom(),
                        createdDateMap.get(getKeyForCreatedDate(other)));
                try {
                    DiscardPriority priority = SubmittedVariantDiscardPolicy.prioritise(currentDeterminants,
                                                                                        otherDeterminants);

                    currentDeterminants = priority.sveToKeep;
                    currentKept = priority.sveToKeep.getSve();
                } catch (IllegalArgumentException exception) {
                    // This is an issue to be investigated but needn't block processing as these SVEs are
                    // indistinguishable, so we log the error and keep whichever is not in the insert list.
                    logger.warn(exception.toString());
                    if (svesToInsert.contains(currentKept) && !svesToInsert.contains(other)) {
                        currentKept = other;
                        currentDeterminants = otherDeterminants;
                    }
                }
            }
            // Discard everything that's not currentKept
            for (SubmittedVariantEntity sve : duplicates) {
                if (sve.equals(currentKept) && sve.getAccession().equals(currentKept.getAccession())) continue;
                // If the SVE to discard isn't one we were trying to insert, add it to the list of SVEs to remove from
                // the database; otherwise just remove it from the list to insert.
                if (!svesToInsert.remove(sve)) {
                    svesToDiscard.add(sve);
                }
                // Either way create a discard operation
                discardOperations.add(getDiscardOperationWithMessage(sve, discardMessage));
            }
        }
        return new ImmutablePair<>(svesToDiscard, discardOperations);
    }

    private Map<String, List<SubmittedVariantEntity>> getDuplicateHashes(List<SubmittedVariantEntity> svesToInsert) {
        List<String> hashes = svesToInsert.stream()
                                          .map(SubmittedVariantEntity::getHashedMessage)
                                          .collect(Collectors.toList());

        // Find duplicate hashes already in db
        List<SubmittedVariantEntity> svesWithSameHash = mongoTemplate.find(query(where("_id").in(hashes)),
                                                                           SubmittedVariantEntity.class, collection);

        // Get the subset of these that are actually identical and remove them immediately from svesToInsert
        // These are counted as skips rather than discards.
        Map<String, List<SubmittedVariantEntity>> duplicateSve = filterForDuplicates(
                Stream.concat(svesToInsert.stream(), svesWithSameHash.stream())
                      .collect(Collectors.groupingBy(sve -> sve.hashCode() + "_" + sve.getAccession())));
        for (List<SubmittedVariantEntity> dups : duplicateSve.values()) {
            if (svesToInsert.remove(dups.get(0))) {
                remappingIngestCounts.addRemappedVariantsSkipped(1);
            }
        }

        // Get the remaining duplicate hashes
        Map<String, List<SubmittedVariantEntity>> svesGroupedByHash =
                Stream.concat(svesToInsert.stream(), svesWithSameHash.stream())
                      .collect(Collectors.groupingBy(SubmittedVariantEntity::getHashedMessage));

        return filterForDuplicates(svesGroupedByHash);
    }

    private Map<Long, List<SubmittedVariantEntity>> getDuplicateAccessions(List<SubmittedVariantEntity> svesToInsert) {
        List<Long> accessions = svesToInsert.stream()
                                            .map(SubmittedVariantEntity::getAccession)
                                            .collect(Collectors.toList());

        // Find duplicate SS already in the database
        List<SubmittedVariantEntity> svesWithSameAccession = mongoTemplate.find(
                query(where("seq").is(assemblyAccession).and("accession").in(accessions)),
                SubmittedVariantEntity.class, collection);

        Map<Long, List<SubmittedVariantEntity>> svesGroupedBySs =
                Stream.concat(svesToInsert.stream(), svesWithSameAccession.stream())
                      .collect(Collectors.groupingBy(SubmittedVariantEntity::getAccession));

        return filterForDuplicates(svesGroupedBySs);
    }

    private Map<ImmutableTriple<String, Long, String>, LocalDateTime> getAllCreatedDateFromSource(
            List<SubmittedVariantEntity> duplicateSves) {
        // By default use the target SVE's created date.
        // Keyed on (accession, hash, remappedFrom) triplet as that's the only way we can reliably distinguish
        // duplicates in all cases. If there's a duplicate triple almost certainly the createdDates are the same.
        Map<ImmutableTriple<String, Long, String>, LocalDateTime> targetToSourceCreatedDate = duplicateSves
                .stream().collect(Collectors.toMap(this::getKeyForCreatedDate,
                                                   SubmittedVariantEntity::getCreatedDate,
                                                   (cd1, cd2) -> Collections.min(Arrays.asList(cd1, cd2))));

        Map<String, List<SubmittedVariantEntity>> svesBySourceAssembly = duplicateSves
                .stream().collect(Collectors.groupingBy(
                        sve -> Objects.isNull(sve.getRemappedFrom()) ? "" : sve.getRemappedFrom()));

        for (Map.Entry<String, List<SubmittedVariantEntity>> asmAndSves : svesBySourceAssembly.entrySet()) {
            String sourceAsm = asmAndSves.getKey();
            List<SubmittedVariantEntity> svesRemappedFromAsm = asmAndSves.getValue();

            // If not remapped, stick with the target created date
            if (sourceAsm.equals("")) {
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
                targetToSourceCreatedDate.put(getKeyForCreatedDate(sve), getCreatedDateFromSource(sve, sourceSves));
            }
        }

        return targetToSourceCreatedDate;
    }

    private ImmutableTriple<String, Long, String> getKeyForCreatedDate(SubmittedVariantEntity sve) {
        return new ImmutableTriple<>(sve.getHashedMessage(), sve.getAccession(), sve.getRemappedFrom());
    }

    private LocalDateTime getCreatedDateFromSource(SubmittedVariantEntity targetSve,
                                                   List<SubmittedVariantEntity> sourceSves) {
        // If we can't find the source SVE, use the created date of the remapped SVE
        if (Objects.isNull(sourceSves) || sourceSves.isEmpty()) {
            logger.warn("No SS " + targetSve.getAccession() + " found in source assembly "
                                + targetSve.getRemappedFrom());
            return targetSve.getCreatedDate();
        }
        // If we find multiple SS in the source, return the min created date
        if (sourceSves.size() > 1) {
            logger.warn("Duplicate SS " + targetSve.getAccession() + " found in source assembly "
                                + targetSve.getRemappedFrom());
            return Collections.min(sourceSves, Comparator.comparing(SubmittedVariantEntity::getCreatedDate))
                              .getCreatedDate();
        }
        return sourceSves.get(0).getCreatedDate();
    }

    private SubmittedVariantOperationEntity getDiscardOperationWithMessage(SubmittedVariantEntity sve, String message) {
        String svoeId = String.format("DISCARD_SS_%s_HASH_%s_SOURCE_%s",
                                      sve.getAccession(),
                                      sve.getHashedMessage(),
                                      sve.getRemappedFrom());
        SubmittedVariantOperationEntity svoe = new SubmittedVariantOperationEntity();
        svoe.fill(EventType.DISCARDED,
                  sve.getAccession(),
                  message,
                  Collections.singletonList(new SubmittedVariantInactiveEntity(sve)));
        svoe.setId(svoeId);
        return svoe;
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
