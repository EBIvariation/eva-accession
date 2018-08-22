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
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
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

    private MongoTemplate mongoTemplate;

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private DbsnpSubmittedVariantOperationWriter dbsnpSubmittedVariantOperationWriter;

    private DbsnpClusteredVariantDeclusteredWriter dbsnpClusteredVariantDeclusteredWriter;

    public DbsnpVariantsWriter(MongoTemplate mongoTemplate, ImportCounts importCounts) {
        this.mongoTemplate = mongoTemplate;
        this.dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate, importCounts);
        this.dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate, importCounts);
        this.dbsnpSubmittedVariantOperationWriter = new DbsnpSubmittedVariantOperationWriter(mongoTemplate,
                                                                                             importCounts);
        this.dbsnpClusteredVariantDeclusteredWriter = new DbsnpClusteredVariantDeclusteredWriter(mongoTemplate);
    }

    @Override
    public void write(List<? extends DbsnpVariantsWrapper> wrappers) throws Exception {
        Set<DbsnpClusteredVariantEntity> clusteredVariantsDeclustered = new HashSet<>();
        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : wrappers) {
            List<DbsnpSubmittedVariantEntity> submittedVariants = dbsnpVariantsWrapper.getSubmittedVariants();
            try {
                dbsnpSubmittedVariantWriter.write(submittedVariants);
            } catch (BulkOperationException exception) {
                addMergeOperationsToWrapper(dbsnpVariantsWrapper, submittedVariants, exception);
            }

            List<DbsnpSubmittedVariantOperationEntity> operations = dbsnpVariantsWrapper.getOperations();
            if (operations != null && !operations.isEmpty()) {
                dbsnpSubmittedVariantOperationWriter.write(operations);
                clusteredVariantsDeclustered.add(dbsnpVariantsWrapper.getClusteredVariant());
            }
        }
        writeClusteredVariants(wrappers);
        writeClusteredVariantsDeclustered(new ArrayList<>(clusteredVariantsDeclustered));
    }

    private void addMergeOperationsToWrapper(DbsnpVariantsWrapper dbsnpVariantsWrapper,
                                             List<DbsnpSubmittedVariantEntity> submittedVariants,
                                             BulkOperationException e) {
        if (dbsnpVariantsWrapper.getOperations() == null) {
            dbsnpVariantsWrapper.setOperations(new ArrayList<>());
        }
        e.getErrors().stream().filter(this::isDuplicateKeyError).forEach(error -> {
            Matcher matcher = DUPLICATE_KEY_PATTERN.matcher(error.getMessage());

            if (matcher.find()) {
                String hash = matcher.group(DUPLICATE_KEY_ERROR_MESSAGE_GROUP_NAME);

                DbsnpSubmittedVariantEntity mergedInto = mongoTemplate.findOne(
                        new Query().addCriteria(Criteria.where("_id").is(hash)),
                        DbsnpSubmittedVariantEntity.class);
                Stream<DbsnpSubmittedVariantOperationEntity> merges = buildMergeOperations(submittedVariants,
                                                                                           hash,
                                                                                           mergedInto);

                merges.forEach(mergeOperation -> dbsnpVariantsWrapper.getOperations().add(mergeOperation));
            } else {
                throw new IllegalStateException(
                        "A duplicate key exception was caught, but the message couldn't be parsed correctly", e);
            }
        });
    }

    /**
     * Build a list of merge operations for submitted variants with the same hash and different subsnp accession.
     *
     * If the subsnp accession is the same, it doesn't make sense to merge it into itself, so the duplicate exception
     * can be ignored.
     */
    private Stream<DbsnpSubmittedVariantOperationEntity> buildMergeOperations(
            List<DbsnpSubmittedVariantEntity> submittedVariants, String hash, DbsnpSubmittedVariantEntity mergedInto) {
        // TODO Multiple accession origin*s*: only one duplicate is thrown per hash
        // so multiple duplicates could be present in a single chunk
        return submittedVariants
                .stream()
                .filter(v -> v.getHashedMessage().equals(hash) && !v.getAccession().equals(mergedInto.getAccession()))
                .map(origin -> buildMergeOperation(origin, mergedInto));
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

    private void writeClusteredVariants(List<? extends DbsnpVariantsWrapper> items) {
        Collection<DbsnpClusteredVariantEntity> uniqueClusteredVariants =
                items.stream()
                     .map(DbsnpVariantsWrapper::getClusteredVariant)
                     .collect(Collectors.toMap(DbsnpClusteredVariantEntity::getHashedMessage,
                                               a -> a,
                                               (a, b) -> a))
                     .values();
        dbsnpClusteredVariantWriter.write(new ArrayList<>(uniqueClusteredVariants));
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
