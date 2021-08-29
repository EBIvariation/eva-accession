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
package uk.ac.ebi.eva.accession.release.batch.io;

import htsjdk.variant.variantcontext.VariantContext;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.BulkOperations;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.query.BasicQuery;
import org.springframework.data.mongodb.core.query.BasicUpdate;

import uk.ac.ebi.eva.accession.core.model.ReleaseRecordEntity;
import uk.ac.ebi.eva.accession.core.model.ReleaseRecordSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Writes a release record to MongoDB
 *
 */
public class ReleaseRecordWriter implements ItemWriter<VariantContext> {

    private static final String ID = "_id";
    private static final String ADD_TO_SET = "$addToSet";
    private static final String SET_ON_INSERT = "$setOnInsert";
    private static final String DOES_NOT_EQUAL = "$ne";

    private static final Logger logger = LoggerFactory.getLogger(ReleaseRecordWriter.class);

    private final MongoOperations mongoOperations;

    private final SubmittedVariantAccessioningRepository submittedVariantAccessioningRepository;

    private final ClusteredVariantAccessioningRepository clusteredVariantAccessioningRepository;

    private final String assemblyAccession;

    public static final String RELEASE_RECORD_COLLECTION_NAME = "releaseRecordEntity";

    public ReleaseRecordWriter(MongoOperations mongoOperations,
                               SubmittedVariantAccessioningRepository submittedVariantAccessioningRepository,
                               ClusteredVariantAccessioningRepository clusteredVariantAccessioningRepository,
                               String assemblyAccession) {
        this.mongoOperations = mongoOperations;
        this.submittedVariantAccessioningRepository = submittedVariantAccessioningRepository;
        this.clusteredVariantAccessioningRepository = clusteredVariantAccessioningRepository;
        this.assemblyAccession = assemblyAccession;
    }

    private void checkSSIDsAreClustered(List<SubmittedVariantEntity> submittedVariantEntities) {
        List<SubmittedVariantEntity> listSSWithMissingRS = submittedVariantEntities
                .stream()
                .filter(submittedVariantEntity -> submittedVariantEntity.getClusteredVariantAccession() == null)
                .collect(Collectors.toList());
        if (listSSWithMissingRS.size() != 0) {
            String ssWithMissingRS = listSSWithMissingRS.stream().map(Object::toString)
                                                        .collect(Collectors.joining(","));
            throw new IllegalStateException("Following SS IDs are not clustered. Was clustering successful?\n" +
                                                    ssWithMissingRS);
        }

    }

    private void checkSSIDsAreInAccessioningWarehouse(List<Long> ssIds,
                                                      List<SubmittedVariantEntity> submittedVariantEntities) {
        if (ssIds.size() != submittedVariantEntities.size()) {
            List<Long> ssIdsFromAccessioningWarehouse = submittedVariantEntities
                    .stream().map(SubmittedVariantEntity::getAccession).collect(Collectors.toList());
            String absentssIdsInAccessioningWarehouse =
                    ssIds.stream().filter(ssId -> !ssIdsFromAccessioningWarehouse.contains(ssId))
                         .map(Object::toString).collect(Collectors.joining(","));
            throw new IllegalStateException("Following SS IDs could not be found in " +
                                                    "the accessioning warehouse. Was accessioning successful?\n" +
                                                    absentssIdsInAccessioningWarehouse);
        }
    }

    private ReleaseRecordEntity constructReleaseRecord(Pair<ClusteredVariantEntity, List<SubmittedVariantEntity>>
                                                               variantRecord) {
        ClusteredVariantEntity clusteredVariantEntity = variantRecord.getLeft();
        String releaseRecordID = String.format("%s_%s", this.assemblyAccession, clusteredVariantEntity.getAccession());
        List<ReleaseRecordSubmittedVariantEntity> releaseRecordSubmittedVariantEntities =
                variantRecord.getRight()
                             .stream()
                             .map(entity -> new ReleaseRecordSubmittedVariantEntity(entity.getAccession(),
                                                                                    entity.getHashedMessage(),
                                                                                    entity.getProjectAccession(),
                                                                                    entity.getContig(),
                                                                                    entity.getStart(),
                                                                                    entity.getReferenceAllele(),
                                                                                    entity.getAlternateAllele(),
                                                                                    entity.isSupportedByEvidence(),
                                                                                    entity.isAssemblyMatch(),
                                                                                    entity.isAllelesMatch(),
                                                                                    entity.isValidated()))
                        .collect(Collectors.toList());
        return new ReleaseRecordEntity(releaseRecordID,
                                       clusteredVariantEntity.getAccession(),
                                       clusteredVariantEntity.getHashedMessage(),
                                       clusteredVariantEntity.getAssemblyAccession(),
                                       clusteredVariantEntity.getTaxonomyAccession(),
                                       clusteredVariantEntity.getContig(), clusteredVariantEntity.getStart(),
                                       clusteredVariantEntity.getType(), clusteredVariantEntity.isValidated(),
                                       clusteredVariantEntity.getMapWeight(), releaseRecordSubmittedVariantEntities);
    }

    private Map<Long, Pair<ClusteredVariantEntity, List<SubmittedVariantEntity>>> joinRSWithSSInfo(
            Map<Long, ClusteredVariantEntity> clusteredVariantEntityMap,
            List<SubmittedVariantEntity> submittedVariantEntities) {
        Map<Long, Pair<ClusteredVariantEntity, List<SubmittedVariantEntity>>> releaseInfo = new HashMap<>();
        for (SubmittedVariantEntity submittedVariantEntity: submittedVariantEntities) {
            Long rsID = submittedVariantEntity.getClusteredVariantAccession();
            if (releaseInfo.containsKey(rsID)) {
                releaseInfo.get(rsID).getRight().add(submittedVariantEntity);
            } else {
                releaseInfo.put(rsID, new MutablePair<>(clusteredVariantEntityMap.get(rsID),
                                                        new ArrayList<>(
                                                                Collections.singletonList(submittedVariantEntity))));
            }
        }
        return releaseInfo;
    }

    /**
     * Get both SS and RS information (termed as a "release record") given a set of SS IDs
     * @param ssIds - List of SS IDs
     * @return A dictionary of RS and its constituent SS - keyed by the RS ID
     */
    private List<ReleaseRecordEntity> getReleaseRecords(List<Long> ssIds) {
        List<SubmittedVariantEntity> submittedVariantEntities =
                submittedVariantAccessioningRepository.findByReferenceSequenceAccessionAndAccessionIn(
                        this.assemblyAccession, ssIds);
        // Ensure that all SS IDs were retrievable from the accessioning warehouse. Fail otherwise.
        checkSSIDsAreInAccessioningWarehouse(ssIds, submittedVariantEntities);
        // Ensure that all SS IDs are clustered in the accessioning warehouse. Fail otherwise.
        checkSSIDsAreClustered(submittedVariantEntities);


        List<Long> rsIdsToLookFor = submittedVariantEntities.stream()
                                                            .map(SubmittedVariantEntity::getClusteredVariantAccession)
                                                            .distinct().collect(Collectors.toList());
        Map<Long, ClusteredVariantEntity> clusteredVariantEntityMap =
                clusteredVariantAccessioningRepository
                        .findByAssemblyAccessionAndAccessionIn(this.assemblyAccession, rsIdsToLookFor)
                        .stream()
                        .collect(Collectors.toMap(ClusteredVariantEntity::getAccession, entity -> entity));
        return joinRSWithSSInfo(clusteredVariantEntityMap, submittedVariantEntities)
                .values().stream().map(this::constructReleaseRecord).collect(Collectors.toList());
    }

    private void upsertReleaseRecordToMongo(BulkOperations bulkOperations, ReleaseRecordEntity releaseRecordEntity) {
        Document releaseRecordIDExists = new Document(ID, releaseRecordEntity.getID());
        Document releaseRecordDocument = new Document();
        mongoOperations.getConverter().write(releaseRecordEntity, releaseRecordDocument);
        Document rsRecordToInsert = new Document(SET_ON_INSERT, releaseRecordDocument);
        bulkOperations.upsert(new BasicQuery(releaseRecordIDExists), new BasicUpdate(rsRecordToInsert));
    }

    private void upsertSSInfoRecordToMongo(BulkOperations bulkOperations, ReleaseRecordEntity releaseRecordEntity) {
        Document releaseRecordIDExists = new Document(ID, releaseRecordEntity.getID());
        String ssIDField = String.format("%s.%s", ReleaseRecordEntity.SS_INFO_FIELD, "accession");
        //Insert only missing SS info sub-document for release records where RS IDs already exist
        for (ReleaseRecordSubmittedVariantEntity releaseRecordSubmittedVariantEntity:
                releaseRecordEntity.getAssociatedSubmittedVariantEntities()) {
            List<Document> ssDoesNotExistCriteria =  new ArrayList<>();
            ssDoesNotExistCriteria.add(releaseRecordIDExists);
            ssDoesNotExistCriteria.add(new Document(ssIDField,
                                                    new Document(DOES_NOT_EQUAL,
                                                    releaseRecordSubmittedVariantEntity.getAccession())));
            Document ssDoesNotExistQuery = new Document("$and", ssDoesNotExistCriteria);
            Document ssRecordtoInsert = new Document();
            mongoOperations.getConverter().write(releaseRecordSubmittedVariantEntity, ssRecordtoInsert);
            Document addSSInfo = new Document(ADD_TO_SET, new Document(ReleaseRecordEntity.SS_INFO_FIELD,
                                                                       ssRecordtoInsert));
            bulkOperations.updateOne(new BasicQuery(ssDoesNotExistQuery), new BasicUpdate(addSSInfo));
        }
    }

    @Override
    public void write(List<? extends VariantContext> variantContexts) throws Exception {
        List<Long> ssIDsToLookFor =  variantContexts.stream()
                                                    .map(VariantContext::getID)
                                                    .map(ssID -> Long.parseLong(ssID.substring(2)))
                                                    .collect(Collectors.toList());
        List<ReleaseRecordEntity> releaseRecords = getReleaseRecords(ssIDsToLookFor);

        //Insert full release record for RS IDs which don't even exist in the target collection
        BulkOperations bulkOperationsToUpsertFullReleaseRecords = mongoOperations.bulkOps(
                BulkOperations.BulkMode.UNORDERED, RELEASE_RECORD_COLLECTION_NAME);
        releaseRecords.forEach(releaseRecord -> upsertReleaseRecordToMongo(bulkOperationsToUpsertFullReleaseRecords,
                                                                           releaseRecord));
        bulkOperationsToUpsertFullReleaseRecords.execute();

        //Insert only SS Info sub-documents where RS IDs are present but a specific SS is not
        /* ex: Release record status
         *
         * Before insert: {"_id": "ASM1_RS1", "accession": "RS1", ..., "ssInfo": [{"accession": "SS1", ...}]
         *
         * After inserting SS2: {"_id": "ASM1_RS1", "accession": "RS1", ...,
         *                                "ssInfo": [{"accession": "SS1", ..., }, {"accession": "SS2", ..., }]
         *
         */
        BulkOperations bulkOperationsToUpsertSSInfoRecords = mongoOperations.bulkOps(
                BulkOperations.BulkMode.UNORDERED, RELEASE_RECORD_COLLECTION_NAME);
        releaseRecords.forEach(releaseRecord -> upsertSSInfoRecordToMongo(bulkOperationsToUpsertSSInfoRecords,
                                                                          releaseRecord));
        bulkOperationsToUpsertSSInfoRecords.execute();
    }
}

