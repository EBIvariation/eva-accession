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
package uk.ac.ebi.eva.accession.dbsnp.batch.io;

import com.mongodb.MongoBulkWriteException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.util.Pair;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.batch.io.DbsnpClusteredVariantOperationWriter;
import uk.ac.ebi.eva.accession.core.batch.io.DbsnpClusteredVariantWriter;
import uk.ac.ebi.eva.accession.core.batch.io.MergeOperationBuilder;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.batch.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantsWrapper;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static uk.ac.ebi.eva.accession.dbsnp.batch.io.DbsnpClusteredVariantDeclusteredWriter.DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME;

public class DbsnpVariantsWriter implements ItemWriter<DbsnpVariantsWrapper> {

    private static final Logger logger = LoggerFactory.getLogger(DbsnpVariantsWriter.class);

    private final MongoTemplate mongoTemplate;

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    private DbsnpSubmittedVariantOperationWriter dbsnpSubmittedVariantOperationWriter;

    private DbsnpClusteredVariantOperationWriter dbsnpClusteredVariantOperationWriter;

    private DbsnpClusteredVariantDeclusteredWriter dbsnpClusteredVariantDeclusteredWriter;

    private MergeOperationBuilder<DbsnpSubmittedVariantEntity, DbsnpSubmittedVariantOperationEntity>
            submittedOperationBuilder;

    private MergeOperationBuilder<DbsnpClusteredVariantEntity, DbsnpClusteredVariantOperationEntity>
            clusteredOperationBuilder;

    private MergeOperationBuilder<DbsnpClusteredVariantEntity, DbsnpClusteredVariantOperationEntity>
            declusteredOperationBuilder;

    public DbsnpVariantsWriter(MongoTemplate mongoTemplate,
                               DbsnpSubmittedVariantOperationRepository submittedOperationRepository,
                               DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository,
                               DbsnpClusteredVariantOperationRepository clusteredOperationRepository,
                               DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository,
                               ImportCounts importCounts) {
        this.mongoTemplate = mongoTemplate;
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
        this.declusteredOperationBuilder = new MergeOperationBuilder<>(
                clusteredOperationRepository,
                id -> java.util.Optional.ofNullable(
                        mongoTemplate.findById(id, DbsnpClusteredVariantEntity.class,
                                               DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME)),
                this::buildClusteredMergeOperation);
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
        List<DbsnpClusteredVariantEntity> declusteredClusteredVariants = new ArrayList<>();
        List<DbsnpSubmittedVariantOperationEntity> declusterOperations = new ArrayList<>();

        for (DbsnpVariantsWrapper dbsnpVariantsWrapper : wrappers) {
            List<DbsnpSubmittedVariantOperationEntity> otherOperations = dbsnpVariantsWrapper.getOperations();
            if (otherOperations != null && !otherOperations.isEmpty()) {
                declusterOperations.addAll(otherOperations);
                declusteredClusteredVariants.add(dbsnpVariantsWrapper.getClusteredVariant());
            }
        }

        List<DbsnpClusteredVariantOperationEntity> clusteredVariantsMergeOperations =
                writeClusteredVariantsAndOperations(wrappers, declusteredClusteredVariants);

        writeSubmittedVariantsAndOperations(wrappers, declusterOperations, clusteredVariantsMergeOperations);
    }

    private List<DbsnpClusteredVariantOperationEntity> writeClusteredVariantsAndOperations(
            List<? extends DbsnpVariantsWrapper> wrappers,
            List<DbsnpClusteredVariantEntity> declusteredClusteredVariants) throws Exception {
        List<DbsnpClusteredVariantOperationEntity> mergeClusteredOperations = new ArrayList<>();

        mergeClusteredOperations.addAll(writeDeclusteredClusteredVariants(declusteredClusteredVariants));
        mergeClusteredOperations.addAll(writeClusteredVariants(wrappers));

        if (!mergeClusteredOperations.isEmpty()) {
            dbsnpClusteredVariantOperationWriter.write(mergeClusteredOperations);
        }
        return mergeClusteredOperations;
    }

    private List<DbsnpClusteredVariantOperationEntity> writeDeclusteredClusteredVariants(
            List<DbsnpClusteredVariantEntity> declusteredClusteredVariants) {
        List<DbsnpClusteredVariantEntity> clusteredVariantsDeclustered = new ArrayList<>(declusteredClusteredVariants);
        try {
            if (!clusteredVariantsDeclustered.isEmpty()) {
                dbsnpClusteredVariantDeclusteredWriter.write(clusteredVariantsDeclustered);
            }
            return Collections.emptyList();
        } catch (DuplicateKeyException exception) {
            MongoBulkWriteException writeException = ((MongoBulkWriteException) exception.getCause());
            return declusteredOperationBuilder.buildMergeOperationsFromException(clusteredVariantsDeclustered,
                                                                                 writeException);
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
        } catch (DuplicateKeyException exception) {
            MongoBulkWriteException writeException = ((MongoBulkWriteException) exception.getCause());
            return clusteredOperationBuilder.buildMergeOperationsFromException(clusteredVariants, writeException);
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

    private void writeSubmittedVariantsAndOperations(List<? extends DbsnpVariantsWrapper> wrappers,
                                                     List<DbsnpSubmittedVariantOperationEntity> declusterOperations,
                                                     List<DbsnpClusteredVariantOperationEntity> mergeClusteredOperations)
            throws Exception {
        List<DbsnpSubmittedVariantOperationEntity> updatedClusteredVariantOperation =
                updateClusteredVariantAccessionsInSubmittedVariants(wrappers, mergeClusteredOperations);

        if (!updatedClusteredVariantOperation.isEmpty()) {
            dbsnpSubmittedVariantOperationWriter.write(updatedClusteredVariantOperation);
        }

        if (!declusterOperations.isEmpty()) {
            dbsnpSubmittedVariantOperationWriter.write(declusterOperations);
        }

        List<DbsnpSubmittedVariantOperationEntity> mergeSubmittedOperations = writeSubmittedVariants(wrappers);
        if (!mergeSubmittedOperations.isEmpty()) {
            dbsnpSubmittedVariantOperationWriter.write(mergeSubmittedOperations);
        }
    }

    private List<DbsnpSubmittedVariantOperationEntity> updateClusteredVariantAccessionsInSubmittedVariants(
            List<? extends DbsnpVariantsWrapper> wrappers,
            List<DbsnpClusteredVariantOperationEntity> mergeClusteredOperations) {

        Map<Pair<String, Long>, Long> replacements = getAccessionReplacements(mergeClusteredOperations);
        List<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();

        for (DbsnpVariantsWrapper wrapper : wrappers) {
            updateClusteredVariantAccessionsInSubmittedVariants(wrapper, replacements, operations);
        }
        return operations;
    }

    /**
     * @return a mapping of where a clustered variant was merged: map from hash+accession to which accession it was
     * merged into.
     */
    private Map<Pair<String, Long>, Long> getAccessionReplacements(
            List<DbsnpClusteredVariantOperationEntity> mergeClusteredOperations) {
        Map<Pair<String, Long>, List<DbsnpClusteredVariantOperationEntity>> accessionToOperationsMap = new HashMap<>();
        for (DbsnpClusteredVariantOperationEntity mergeClusteredOperation : mergeClusteredOperations) {
            String hash = mergeClusteredOperation.getInactiveObjects().get(0).getHashedMessage();
            Long accession = mergeClusteredOperation.getAccession();
            accessionToOperationsMap
                    .computeIfAbsent(Pair.of(hash, accession), k -> new ArrayList<>())
                    .add(mergeClusteredOperation);
        }

        Map<Pair<String, Long>, Long> originalToNewAccessions = new HashMap<>();
        accessionToOperationsMap.forEach((hashAndAccession, accessionOperations) -> {
            List<Long> mergedIntoList = accessionOperations
                    .stream()
                    .map(EventDocument::getMergedInto)
                    .distinct()
                    .collect(Collectors.toList());

            if (mergedIntoList.size() > 1) {
                DbsnpClusteredVariantEntity activeClusteredVariant = mongoTemplate.findById(
                        hashAndAccession.getFirst(), DbsnpClusteredVariantEntity.class);

                if (activeClusteredVariant == null || !mergedIntoList.contains(activeClusteredVariant.getAccession())) {
                    throwSeveralInactiveMergesException(hashAndAccession, mergedIntoList, activeClusteredVariant);
                } else {
                    originalToNewAccessions.put(hashAndAccession, activeClusteredVariant.getAccession());
                    logger.debug("Clustered variant rs{} was merged into several other clustered variants with the "
                                         + "same hash: {}. The accession rs{} was chosen as "
                                         + "replacement because it's active, i.e. present in the collection for {}",
                            hashAndAccession.getSecond(), mergedIntoList, activeClusteredVariant.getAccession(),
                                 DbsnpClusteredVariantEntity.class.getSimpleName());
                }
            } else {
                originalToNewAccessions.put(hashAndAccession, mergedIntoList.get(0));
            }
        });
        return originalToNewAccessions;
    }

    private void throwSeveralInactiveMergesException(Pair<String, Long> hashAndAccession, List<Long> mergedIntoList,
                                                     DbsnpClusteredVariantEntity activeClusteredVariant) {
        String activeVariantMessage;
        if (activeClusteredVariant == null) {
            activeVariantMessage = "There is no active variant with that hash.";
        } else {
            activeVariantMessage =
                    "However, there is an active variant with that hash and accession rs"
                            + activeClusteredVariant.getAccession() + ".";

        }
        String accessionsMergedInto = mergedIntoList.stream()
                                                    .map(accession -> "rs" + accession)
                                                    .collect(Collectors.joining(", "));
        throw new IllegalStateException(
                "Clustered variant rs" + hashAndAccession.getSecond() + " was merged into several other "
                        + "clustered variants (" + accessionsMergedInto + ") because all have the same hash ("
                        + hashAndAccession.getFirst() + ") but none of those clustered variants is active, i.e. "
                        + "present in the collection for " + DbsnpClusteredVariantEntity.class.getSimpleName()
                        + ". " + activeVariantMessage);
    }

    private void updateClusteredVariantAccessionsInSubmittedVariants(DbsnpVariantsWrapper wrapper,
                                                                     Map<Pair<String, Long>, Long> replacements,
                                                                     List<DbsnpSubmittedVariantOperationEntity> operations) {
        for (DbsnpSubmittedVariantEntity submittedVariant : wrapper.getSubmittedVariants()) {
            Long mergedInto = replacements.get(Pair.of(wrapper.getClusteredVariant().getHashedMessage(),
                                                       wrapper.getClusteredVariant().getAccession()));
            if (mergedInto != null && submittedVariant.getClusteredVariantAccession() != null) {
                operations.add(buildOperation(submittedVariant, mergedInto));
                submittedVariant.setClusteredVariantAccession(mergedInto);
            }
        }
    }

    private DbsnpSubmittedVariantOperationEntity buildOperation(DbsnpSubmittedVariantEntity originalSubmittedVariant,
                                                                Long clusteredVariantMergedInto) {
        DbsnpSubmittedVariantInactiveEntity inactiveEntity = new DbsnpSubmittedVariantInactiveEntity(
                originalSubmittedVariant);

        Long originalClusteredVariant = originalSubmittedVariant.getClusteredVariantAccession();
        String reason = "Original rs" + originalClusteredVariant + " was merged into rs" + clusteredVariantMergedInto + ".";

        Long accession = originalSubmittedVariant.getAccession();
        DbsnpSubmittedVariantOperationEntity operation = new DbsnpSubmittedVariantOperationEntity();

        // Note the next null in accessionIdDestiny. We are not merging the submitted variant into
        // clusteredVariantMergedInto. We are updating the submitted variant, changing its rs field
        operation.fill(EventType.UPDATED, accession, null, reason, Collections.singletonList(inactiveEntity));
        return operation;
    }

    private List<DbsnpSubmittedVariantOperationEntity> writeSubmittedVariants(
            List<? extends DbsnpVariantsWrapper> wrappers) {
        List<DbsnpSubmittedVariantEntity> submittedVariants = wrappers.stream()
                                                                      .flatMap(w -> w.getSubmittedVariants().stream())
                                                                      .collect(Collectors.toList());
        try {
            dbsnpSubmittedVariantWriter.write(submittedVariants);
            return Collections.emptyList();
        } catch (DuplicateKeyException exception) {
            MongoBulkWriteException writeException = ((MongoBulkWriteException) exception.getCause());
            return submittedOperationBuilder.buildMergeOperationsFromException(submittedVariants, writeException);
        }
    }

}
