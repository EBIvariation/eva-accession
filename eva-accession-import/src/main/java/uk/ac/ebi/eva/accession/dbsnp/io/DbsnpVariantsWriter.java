/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp.io;

import com.mongodb.BulkWriteError;
import com.mongodb.ErrorCategory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbsnpVariantsWriter implements ItemWriter<DbsnpVariantsWrapper> {

    private final String DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME = "IDKEY";

    private final String DUPLICATE_KEY_ERROR_MESSAGE_REGEX = "index:\\s.*\\{\\s?\\:\\s?\"(?<"
            + DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME + ">[a-zA-Z0-9]+)\"\\s?\\}";

    private final Pattern DUPLICATE_KEY_PATTERN = Pattern.compile(DUPLICATE_KEY_ERROR_MESSAGE_REGEX);

    private DbsnpSubmittedVariantOperationRepository submittedOperationRepository;

    private DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository;

    private DbsnpClusteredVariantOperationRepository clusteredOperationRepository;

    private DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository;

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private DbsnpSubmittedVariantOperationWriter dbsnpSubmittedVariantOperationWriter;

    private DbsnpClusteredVariantOperationWriter dbsnpClusteredVariantOperationWriter;

    private DbsnpClusteredVariantDeclusteredWriter dbsnpClusteredVariantDeclusteredWriter;

    public DbsnpVariantsWriter(MongoTemplate mongoTemplate,
                               DbsnpSubmittedVariantOperationRepository submittedOperationRepository,
                               DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository,
                               DbsnpClusteredVariantOperationRepository clusteredOperationRepository,
                               DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository,
                               ImportCounts importCounts) {
        this.submittedOperationRepository = submittedOperationRepository;
        this.submittedVariantRepository = submittedVariantRepository;
        this.clusteredOperationRepository = clusteredOperationRepository;
        this.clusteredVariantRepository = clusteredVariantRepository;
        this.dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate, importCounts);
        this.dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate, importCounts);
        this.dbsnpSubmittedVariantOperationWriter = new DbsnpSubmittedVariantOperationWriter(mongoTemplate,
                                                                                             importCounts);
        this.dbsnpClusteredVariantOperationWriter= new DbsnpClusteredVariantOperationWriter(mongoTemplate,
                                                                                             importCounts);
        this.dbsnpClusteredVariantDeclusteredWriter = new DbsnpClusteredVariantDeclusteredWriter(mongoTemplate);
    }

    @Override
    public void write(List<? extends DbsnpVariantsWrapper> wrappers) throws Exception {
        Set<DbsnpClusteredVariantEntity> clusteredVariantsDeclustered = new HashSet<>();
        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : wrappers) {
            List<DbsnpSubmittedVariantOperationEntity> operations = dbsnpVariantsWrapper.getOperations();
            if (operations != null && !operations.isEmpty()) {
                dbsnpSubmittedVariantOperationWriter.write(operations);
                clusteredVariantsDeclustered.add(dbsnpVariantsWrapper.getClusteredVariant());
            }

            List<DbsnpSubmittedVariantOperationEntity> mergeOperations = writeSubmittedVariants(dbsnpVariantsWrapper);
            if (!mergeOperations.isEmpty()) {
                dbsnpSubmittedVariantOperationWriter.write(mergeOperations);
            }
        }
        List<DbsnpClusteredVariantOperationEntity> mergeOperations = writeClusteredVariants(wrappers);
        if (!mergeOperations.isEmpty()) {
            dbsnpClusteredVariantOperationWriter.write(mergeOperations);
        }
        writeClusteredVariantsDeclustered(new ArrayList<>(clusteredVariantsDeclustered));
    }

    private List<DbsnpSubmittedVariantOperationEntity> writeSubmittedVariants(
            DbsnpVariantsWrapper dbsnpVariantsWrapper) {
        List<DbsnpSubmittedVariantEntity> submittedVariants = dbsnpVariantsWrapper.getSubmittedVariants();
        try {
            dbsnpSubmittedVariantWriter.write(submittedVariants);
            return Collections.emptyList();
        } catch (BulkOperationException exception) {
            return buildMergeOperations(submittedVariants, exception);
        }
    }

    private List<DbsnpSubmittedVariantOperationEntity> buildMergeOperations(
            List<DbsnpSubmittedVariantEntity> submittedVariants, BulkOperationException exception) {
        List<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        extractUniqueHashes(exception)
                .forEach(hash -> {
                    DbsnpSubmittedVariantEntity mergedInto = submittedVariantRepository.findOne(hash);
                    List<DbsnpSubmittedVariantOperationEntity> merges = buildMergeOperations(submittedVariants, hash,
                                                                                             mergedInto);

                    operations.addAll(merges);
                });
        return operations;
    }

    private Stream<String> extractUniqueHashes(BulkOperationException exception) {
        return exception.getErrors()
                 .stream()
                 .filter(this::isDuplicateKeyError)
                 .map(error -> {
                     Matcher matcher = DUPLICATE_KEY_PATTERN.matcher(error.getMessage());

                     if (!matcher.find()) {
                         throw new IllegalStateException(
                                 "A duplicate key exception was caught, but the message couldn't be parsed correctly",
                                 exception);
                     } else {
                         String hash = matcher.group(DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME);
                         return hash;
                     }
                 })
                 .distinct();
    }

    /**
     * Build a list of merge operations for *distinct* new submitted variants that will be merged into existing
     * submitted variants.
     *
     * Two submitted variants have to be merged if they have the same hash and different subsnp accession, and only if
     * the new one wasn't merged before.
     *
     * If the subsnp accession is the same, it doesn't make sense to merge it into itself, so the duplicate exception
     * can be ignored.
     */
    private List<DbsnpSubmittedVariantOperationEntity> buildMergeOperations(
            List<DbsnpSubmittedVariantEntity> submittedVariants, String hash, DbsnpSubmittedVariantEntity mergedInto) {
        return removeDuplicatesWithSameHashAndAccession(submittedVariants.stream())
                .stream()
                .filter(v -> v.getHashedMessage().equals(hash)
                        && !v.getAccession().equals(mergedInto.getAccession())
                        && !isSubmittedVariantAlreadyMerged(v.getAccession()))
                .map(origin -> buildMergeOperation(origin, mergedInto))
                .collect(Collectors.toList());
    }

    private <T extends AccessionedDocument> Collection<T> removeDuplicatesWithSameHashAndAccession(
            Stream<T> accessionedEntities) {
        return accessionedEntities.collect(Collectors.toMap(v -> v.getHashedMessage() + v.getAccession().toString(),
                                                            v -> v,
                                                            (a, b) -> a))
                                  .values();
    }

    private boolean isSubmittedVariantAlreadyMerged(Long accession) {
        List<DbsnpSubmittedVariantOperationEntity> merges = submittedOperationRepository.findAllByAccession(accession);
        return !merges.isEmpty();
    }

    private DbsnpSubmittedVariantOperationEntity buildMergeOperation(DbsnpSubmittedVariantEntity origin,
                                                                     DbsnpSubmittedVariantEntity mergedInto) {
        DbsnpSubmittedVariantInactiveEntity inactiveEntity = new DbsnpSubmittedVariantInactiveEntity(origin);

        // TODO Use library if possible
        DbsnpSubmittedVariantOperationEntity operation = new DbsnpSubmittedVariantOperationEntity();
        operation.fill(EventType.MERGED, origin.getAccession(), mergedInto.getAccession(),
                       "Identical submitted variant received multiple SS identifiers",
                       Arrays.asList(inactiveEntity));

        return operation;
    }

    private List<DbsnpClusteredVariantOperationEntity> writeClusteredVariants(
            List<? extends DbsnpVariantsWrapper> items) {
        List<DbsnpClusteredVariantEntity> uniqueClusteredVariants = new ArrayList<>(removeDuplicatesWithSameHashAndAccession(
                items.stream().map(DbsnpVariantsWrapper::getClusteredVariant)));
        try {
            dbsnpClusteredVariantWriter.write(uniqueClusteredVariants);
            return Collections.emptyList();
        } catch (BulkOperationException exception) {
            return buildClusteredMergeOperations(uniqueClusteredVariants, exception);
        }
    }

    private List<DbsnpClusteredVariantOperationEntity> buildClusteredMergeOperations(
            List<DbsnpClusteredVariantEntity> clusteredVariants, BulkOperationException exception) {
        List<DbsnpClusteredVariantOperationEntity> operations = new ArrayList<>();
        extractUniqueHashes(exception)
                .forEach(hash -> {
                    DbsnpClusteredVariantEntity mergedInto = clusteredVariantRepository.findOne(hash);
                    List<DbsnpClusteredVariantOperationEntity> merges = buildClusteredMergeOperations(clusteredVariants,
                                                                                                      hash,
                                                                                                      mergedInto);

                    operations.addAll(merges);
                });
        return operations;

    }

    /**
     * Build a list of merge operations for *distinct* new submitted variants that will be merged into existing
     * submitted variants.
     *
     * Two submitted variants have to be merged if they have the same hash and different subsnp accession, and only if
     * the new one wasn't merged before.
     *
     * If the subsnp accession is the same, it doesn't make sense to merge it into itself, so the duplicate exception
     * can be ignored.
     */
    private List<DbsnpClusteredVariantOperationEntity> buildClusteredMergeOperations(
            List<DbsnpClusteredVariantEntity> clusteredVariants, String hash, DbsnpClusteredVariantEntity mergedInto) {
        return removeDuplicatesWithSameHashAndAccession(clusteredVariants.stream())
                .stream()
                .filter(v -> v.getHashedMessage().equals(hash)
                        && !v.getAccession().equals(mergedInto.getAccession())
                        && !isClusteredVariantAlreadyMerged(v.getAccession()))
                .map(origin -> buildMergeOperation(origin, mergedInto))
                .collect(Collectors.toList());
    }

    private boolean isClusteredVariantAlreadyMerged(Long accession) {
        List<DbsnpClusteredVariantOperationEntity> merges = clusteredOperationRepository.findAllByAccession(accession);
        return !merges.isEmpty();
    }

    private DbsnpClusteredVariantOperationEntity buildMergeOperation(DbsnpClusteredVariantEntity origin,
                                                                     DbsnpClusteredVariantEntity mergedInto) {
        DbsnpClusteredVariantInactiveEntity inactiveEntity = new DbsnpClusteredVariantInactiveEntity(origin);

        // TODO Use library if possible
        DbsnpClusteredVariantOperationEntity operation = new DbsnpClusteredVariantOperationEntity();
        operation.fill(EventType.MERGED, origin.getAccession(), mergedInto.getAccession(),
                       "Identical clustered variant received multiple RS identifiers",
                       Arrays.asList(inactiveEntity));

        return operation;
    }
    private void writeClusteredVariantsDeclustered(List<DbsnpClusteredVariantEntity> clusteredVariantsDeclustered) {
        if (!clusteredVariantsDeclustered.isEmpty()) {
            dbsnpClusteredVariantDeclusteredWriter.write(clusteredVariantsDeclustered);
        }
    }

    private boolean isDuplicateKeyError(BulkWriteError error) {
        ErrorCategory errorCategory = ErrorCategory.fromErrorCode(error.getCode());
        return errorCategory.equals(ErrorCategory.DUPLICATE_KEY);
    }
}
