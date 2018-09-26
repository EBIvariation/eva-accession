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
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.repositories.IAccessionedObjectRepository;
import uk.ac.ebi.ampt2d.commons.accession.persistence.repositories.IHistoryRepository;

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
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class DbsnpVariantsWriter implements ItemWriter<DbsnpVariantsWrapper> {

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private DbsnpSubmittedVariantOperationWriter dbsnpSubmittedVariantOperationWriter;

    private DbsnpClusteredVariantOperationWriter dbsnpClusteredVariantOperationWriter;

    private DbsnpClusteredVariantDeclusteredWriter dbsnpClusteredVariantDeclusteredWriter;

    private MergeOperationBuilder<DbsnpSubmittedVariantEntity, DbsnpSubmittedVariantOperationEntity>
            submittedOperationBuilder;

    private MergeOperationBuilder<DbsnpClusteredVariantEntity, DbsnpClusteredVariantOperationEntity>
            clusteredOperationBuilder;

    public DbsnpVariantsWriter(MongoTemplate mongoTemplate,
                               DbsnpSubmittedVariantOperationRepository submittedOperationRepository,
                               DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository,
                               DbsnpClusteredVariantOperationRepository clusteredOperationRepository,
                               DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository,
                               ImportCounts importCounts) {
        this.dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate, importCounts);
        this.dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate, importCounts);
        this.dbsnpSubmittedVariantOperationWriter = new DbsnpSubmittedVariantOperationWriter(mongoTemplate,
                                                                                             importCounts);
        this.dbsnpClusteredVariantOperationWriter= new DbsnpClusteredVariantOperationWriter(mongoTemplate,
                                                                                             importCounts);
        this.dbsnpClusteredVariantDeclusteredWriter = new DbsnpClusteredVariantDeclusteredWriter(mongoTemplate);

        this.submittedOperationBuilder = new MergeOperationBuilder<>(
                submittedOperationRepository, submittedVariantRepository, this::buildSubmittedMergeOperation);
        this.clusteredOperationBuilder = new MergeOperationBuilder<>(
                clusteredOperationRepository, clusteredVariantRepository, this::buildClusteredMergeOperation);
    }

    private DbsnpSubmittedVariantOperationEntity buildSubmittedMergeOperation(DbsnpSubmittedVariantEntity origin,
                                                                              DbsnpSubmittedVariantEntity mergedInto) {
        DbsnpSubmittedVariantInactiveEntity inactiveEntity = new DbsnpSubmittedVariantInactiveEntity(origin);

        DbsnpSubmittedVariantOperationEntity operation = new DbsnpSubmittedVariantOperationEntity();
        operation.fill(EventType.MERGED, origin.getAccession(), mergedInto.getAccession(),
                       "Identical submitted variant received multiple SS identifiers",
                       Arrays.asList(inactiveEntity));

        return operation;
    }

    private DbsnpClusteredVariantOperationEntity buildClusteredMergeOperation(DbsnpClusteredVariantEntity origin,
                                                                              DbsnpClusteredVariantEntity mergedInto) {
        DbsnpClusteredVariantInactiveEntity inactiveEntity = new DbsnpClusteredVariantInactiveEntity(origin);

        DbsnpClusteredVariantOperationEntity operation = new DbsnpClusteredVariantOperationEntity();
        operation.fill(EventType.MERGED, origin.getAccession(), mergedInto.getAccession(),
                       "Identical clustered variant received multiple RS identifiers",
                       Arrays.asList(inactiveEntity));

        return operation;
    }

    @Override
    public void write(List<? extends DbsnpVariantsWrapper> wrappers) throws Exception {
        Set<DbsnpClusteredVariantEntity> clusteredVariantsDeclustered = new HashSet<>();
        List<DbsnpSubmittedVariantOperationEntity> declusterOperations = new ArrayList<>();

        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : wrappers) {
            List<DbsnpSubmittedVariantOperationEntity> otherOperations = dbsnpVariantsWrapper.getOperations();
            if (otherOperations != null && !otherOperations.isEmpty()) {
                declusterOperations.addAll(otherOperations);
                clusteredVariantsDeclustered.add(dbsnpVariantsWrapper.getClusteredVariant());
            }
        }
        writeClusteredVariantsDeclustered(new ArrayList<>(clusteredVariantsDeclustered));
        if (!declusterOperations.isEmpty()) {
            dbsnpSubmittedVariantOperationWriter.write(declusterOperations);
        }

        List<DbsnpSubmittedVariantOperationEntity> mergeSubmittedOperations = writeSubmittedVariants(wrappers);
        if (!mergeSubmittedOperations.isEmpty()) {
            dbsnpSubmittedVariantOperationWriter.write(mergeSubmittedOperations);
        }

        List<DbsnpClusteredVariantOperationEntity> mergeClusteredOperations = writeClusteredVariants(wrappers);
        if (!mergeClusteredOperations.isEmpty()) {
            dbsnpClusteredVariantOperationWriter.write(mergeClusteredOperations);
        }
    }

    private List<DbsnpSubmittedVariantOperationEntity> writeSubmittedVariants(
            List<? extends DbsnpVariantsWrapper> wrappers) {
        List<DbsnpSubmittedVariantEntity> submittedVariants = wrappers.stream()
                                                                      .flatMap(w -> w.getSubmittedVariants().stream())
                                                                      .collect(Collectors.toList());
        try {
            dbsnpSubmittedVariantWriter.write(submittedVariants);
            return Collections.emptyList();
        } catch (BulkOperationException exception) {
            return submittedOperationBuilder.buildMergeOperationsFromException(submittedVariants, exception);
        }
    }

    private List<DbsnpClusteredVariantOperationEntity> writeClusteredVariants(
            List<? extends DbsnpVariantsWrapper> wrappers) {
        List<DbsnpClusteredVariantEntity> clusteredVariants = getNonDeclusteredClusteredVariants(wrappers);
        try {
            if (!clusteredVariants.isEmpty()) {
                dbsnpClusteredVariantWriter.write(clusteredVariants);
            }
            return Collections.emptyList();
        } catch (BulkOperationException exception) {
            return clusteredOperationBuilder.buildMergeOperationsFromException(clusteredVariants, exception);
        }
    }

    private List<DbsnpClusteredVariantEntity> getNonDeclusteredClusteredVariants(
            List<? extends DbsnpVariantsWrapper> wrappers) {
        return wrappers.stream()
                       .filter(w -> w.getSubmittedVariants()
                                     .stream()
                                     .anyMatch(v -> v.getClusteredVariantAccession() != null))
                       .map(DbsnpVariantsWrapper::getClusteredVariant)
                       .collect(Collectors.toList());
    }

    private void writeClusteredVariantsDeclustered(List<DbsnpClusteredVariantEntity> clusteredVariantsDeclustered) {
        if (!clusteredVariantsDeclustered.isEmpty()) {
            dbsnpClusteredVariantDeclusteredWriter.write(clusteredVariantsDeclustered);
        }
    }

    private class MergeOperationBuilder<ENTITY extends AccessionedDocument<?, Long>,
            OPERATION_ENTITY extends EventDocument<?, Long, ?>> {

        private final String DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME = "IDKEY";

        private final String DUPLICATE_KEY_ERROR_MESSAGE_REGEX = "index:\\s.*\\{\\s?\\:\\s?\"(?<"
                + DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME + ">[a-zA-Z0-9]+)\"\\s?\\}";

        private final Pattern DUPLICATE_KEY_PATTERN = Pattern.compile(DUPLICATE_KEY_ERROR_MESSAGE_REGEX);

        IHistoryRepository<Long, OPERATION_ENTITY, String> operationRepository;

        IAccessionedObjectRepository<ENTITY, Long> variantRepository;

        BiFunction<ENTITY, ENTITY, OPERATION_ENTITY> mergeOperationFactory;

        MergeOperationBuilder(IHistoryRepository<Long, OPERATION_ENTITY, String> operationRepository,
                              IAccessionedObjectRepository<ENTITY, Long> variantRepository,
                              BiFunction<ENTITY, ENTITY, OPERATION_ENTITY> mergeOperationFactory) {
            this.operationRepository = operationRepository;
            this.variantRepository = variantRepository;
            this.mergeOperationFactory = mergeOperationFactory;
        }

        List<OPERATION_ENTITY> buildMergeOperationsFromException(List<ENTITY> variants,
                                                                 BulkOperationException exception) {
            List<OPERATION_ENTITY> operations = new ArrayList<>();
            checkForNulls(variants);
            extractUniqueHashes(exception)
                    .forEach(hash -> {
                        ENTITY mergedInto = variantRepository.findOne(hash);
                        if (mergedInto == null) {
                            throwMongoConsistencyException(variants, hash);
                        }
                        List<OPERATION_ENTITY> merges = buildMergeOperations(variants,
                                                                             hash,
                                                                             mergedInto);

                        operations.addAll(merges);
                    });
            return operations;
        }

        private void throwMongoConsistencyException(List<ENTITY> variants, String hash) {
            String printedVariants = variants
                    .stream()
                    .filter(v -> v.getHashedMessage().equals(hash))
                    .map(v -> v.getClass().toString() + v.getModel().toString())
                    .collect(Collectors.toList())
                    .toString();
            throw new IllegalStateException(
                    "A duplicate key exception was raised with hash " + hash + ", but no document " +
                            "with that hash was found. Make sure you are using ReadPreference=primaryPreferred and "
                            + "WriteConcern=Majority. These variants have that hash: " +
                            printedVariants);
        }

        private Stream<String> extractUniqueHashes(BulkOperationException exception) {
            return exception.getErrors()
                            .stream()
                            .filter(this::isDuplicateKeyError)
                            .map(error -> {
                                Matcher matcher = DUPLICATE_KEY_PATTERN.matcher(error.getMessage());

                                if (!matcher.find()) {
                                    throw new IllegalStateException("A duplicate key exception was caught, but the " +
                                                                            "message couldn't be parsed correctly",
                                                                    exception);
                                } else {
                                    String hash = matcher.group(DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME);
                                    if (hash == null) {
                                        throw new IllegalStateException(
                                                "A duplicate key exception was caught, but the message couldn't be " +
                                                        "parsed correctly. The group in the regex " +
                                                        DUPLICATE_KEY_ERROR_MESSAGE_REGEX + " failed to match part of" +
                                                        " the input",
                                                exception);
                                    }
                                    return hash;
                                }
                            })
                            .distinct();
        }

        private boolean isDuplicateKeyError(BulkWriteError error) {
            ErrorCategory errorCategory = ErrorCategory.fromErrorCode(error.getCode());
            return errorCategory.equals(ErrorCategory.DUPLICATE_KEY);
        }

        private List<OPERATION_ENTITY> buildMergeOperations(List<ENTITY> variants, String hash, ENTITY mergedInto) {
            Collection<ENTITY> entities = removeDuplicatesWithSameHashAndAccession(variants.stream());
            checkForNulls(entities);
            return entities
                    .stream()
                    .filter(v -> v.getHashedMessage().equals(hash)
                            && !v.getAccession().equals(mergedInto.getAccession())
                            && !isAlreadyMerged(v.getAccession()))
                    .map(origin -> mergeOperationFactory.apply(origin, mergedInto))
                    .collect(Collectors.toList());
        }

        private void checkForNulls(Collection<ENTITY> entities) {
            int nullCount = 0;
            for (ENTITY entity : entities) {
                if (entity == null) {
                    nullCount++;
                }
            }
            if (nullCount > 0) {
                throw new IllegalStateException(
                        "Could not complete writing merge operations, as " + nullCount + " variants were actually " +
                                "null");
            }
        }

        private <T extends AccessionedDocument> Collection<T> removeDuplicatesWithSameHashAndAccession(
                Stream<T> accessionedEntities) {
            return accessionedEntities.collect(Collectors.toMap(v -> v.getHashedMessage() + v.getAccession().toString(),
                                                                v -> v,
                                                                (a, b) -> a))
                                      .values();
        }

        private boolean isAlreadyMerged(Long accession) {
            List<OPERATION_ENTITY> merges = operationRepository.findAllByAccession(accession);
            return merges.stream().anyMatch(operation -> operation.getEventType().equals(EventType.MERGED));
        }
    }
}
