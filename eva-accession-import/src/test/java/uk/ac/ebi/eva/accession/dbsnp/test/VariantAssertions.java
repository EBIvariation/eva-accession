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
package uk.ac.ebi.eva.accession.dbsnp.test;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.batch.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantsWrapper;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.dbsnp.batch.io.DbsnpClusteredVariantDeclusteredWriter.DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME;

public class VariantAssertions {

    private static final String MERGE_INTO_KEY = "mergeInto";

    private static final String EVENT_TYPE_KEY = "eventType";

    private static final String INACTIVE_OBJECTS_RS_KEY = "inactiveObjects.rs";

    private static final String ACCESSION_KEY = "accession";

    private MongoTemplate mongoTemplate;

    private ImportCounts importCounts;

    public VariantAssertions(MongoTemplate mongoTemplate, ImportCounts importCounts) {
        this.mongoTemplate = mongoTemplate;
        this.importCounts = importCounts;
    }

    public void assertSubmittedVariantsStored(int expectedVariants,
                                                DbsnpSubmittedVariantEntity... submittedVariants) {
        List<DbsnpSubmittedVariantEntity> ssEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(expectedVariants, ssEntities.size());
        assertEquals(expectedVariants, submittedVariants.length);
        assertEquals(expectedVariants, importCounts.getSubmittedVariantsWritten());

        for (int i = 0; i < expectedVariants; i++) {
            if (!ssEntities.contains(submittedVariants[i])) {
                fail("submitted variant was not stored: " + submittedVariants[i] + ".\nFull collection content:"
                        + " " + ssEntities);
            }
        }
    }

    public void assertClusteredVariantStored(int expectedVariants, DbsnpVariantsWrapper... wrappers) {
        List<DbsnpClusteredVariantEntity> rsEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(expectedVariants, rsEntities.size());
        assertEquals(expectedVariants, wrappers.length);
        assertEquals(expectedVariants, importCounts.getClusteredVariantsWritten());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(rsEntities.contains(wrappers[i].getClusteredVariant()));
        }
    }

    public void assertSubmittedVariantDeclusteredStored(DbsnpVariantsWrapper wrapper) {
        List<DbsnpSubmittedVariantEntity> ssEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(1, ssEntities.size());
        assertEquals(1, wrapper.getSubmittedVariants().size());
        assertEquals(wrapper.getSubmittedVariants().get(0), ssEntities.get(0));
        assertNull(ssEntities.get(0).getClusteredVariantAccession());
        assertEquals(1, importCounts.getSubmittedVariantsWritten());
    }

    public void assertDeclusteringHistoryStored(Long clusteredVariantAccession,
                                                 DbsnpSubmittedVariantEntity... dbsnpSubmittedVariantEntities) {
        for (DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity : dbsnpSubmittedVariantEntities) {
            assertNull(dbsnpSubmittedVariantEntity.getClusteredVariantAccession());
        }

        List<DbsnpSubmittedVariantEntity> ssEntities =
                mongoTemplate.find(new Query(), DbsnpSubmittedVariantEntity.class);
        List<DbsnpSubmittedVariantOperationEntity> operationEntities =
                mongoTemplate.find(new Query(), DbsnpSubmittedVariantOperationEntity.class);

        assertNull(ssEntities.get(0).getClusteredVariantAccession());
        assertEquals(ssEntities.get(0).getAccession(), operationEntities.get(0).getAccession());

        assertEquals(1, operationEntities.size());
        assertEquals(EventType.UPDATED, operationEntities.get(0).getEventType());
        assertEquals(1, operationEntities.get(0).getInactiveObjects().size());
        assertEquals(clusteredVariantAccession, operationEntities
                .get(0)
                .getInactiveObjects()
                .get(0)
                .getClusteredVariantAccession());
        assertEquals(1, importCounts.getOperationsWritten());
    }

    public void assertClusteredVariantDeclusteredStored(int expectedVariants, DbsnpVariantsWrapper... wrappers) {
        List<DbsnpClusteredVariantEntity> rsDeclusteredEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertEquals(expectedVariants, rsDeclusteredEntities.size());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(rsDeclusteredEntities.contains(wrappers[i].getClusteredVariant()));
        }
    }

    public void assertClusteredVariantMergeOperationStored(int expectedTotalOperations, int expectedMatchingOperations,
                                                            DbsnpClusteredVariantEntity mergedInto) {
        List<DbsnpClusteredVariantOperationEntity> totalOperationEntities = mongoTemplate.findAll(
                DbsnpClusteredVariantOperationEntity.class);
        if (expectedTotalOperations != totalOperationEntities.size()) {
            fail("Expected " + expectedTotalOperations + " clustered variant operations, there are "
                         + totalOperationEntities.size() + ".\nFull collection content: " + totalOperationEntities);
        }

        List<DbsnpClusteredVariantOperationEntity> operationEntities = mongoTemplate.find(
                query(where(MERGE_INTO_KEY).is(mergedInto.getAccession())), DbsnpClusteredVariantOperationEntity.class);
        if (expectedMatchingOperations != operationEntities.size()) {
            fail("Expected " + expectedMatchingOperations + " clustered variant operations, there are "
                         + operationEntities.size() + ".\nFull collection content: " + totalOperationEntities);
        }

        operationEntities.forEach(operation -> {
            assertEquals(EventType.MERGED, operation.getEventType());
            List<DbsnpClusteredVariantInactiveEntity> inactiveObjects = operation.getInactiveObjects();
            assertEquals(1, inactiveObjects.size());
            DbsnpClusteredVariantInactiveEntity inactiveEntity = inactiveObjects.get(0);
            assertNotEquals(mergedInto.getAccession(), inactiveEntity.getAccession());

            assertEquals(mergedInto.getAssemblyAccession(), inactiveEntity.getAssemblyAccession());
            assertEquals(mergedInto.getContig(), inactiveEntity.getContig());
            assertEquals(mergedInto.getStart(), inactiveEntity.getStart());
            assertEquals(mergedInto.getTaxonomyAccession(), inactiveEntity.getTaxonomyAccession());
            assertEquals(mergedInto.getType(), inactiveEntity.getType());
            assertEquals(mergedInto.isValidated(), inactiveEntity.isValidated());
        });
    }

    public void assertSubmittedVariantMergeOperationStored(int expectedTotalOperations, int expectedMatchingOperations,
                                                            DbsnpSubmittedVariantEntity mergedInto) {
        List<DbsnpSubmittedVariantOperationEntity> totalOperationEntities = mongoTemplate.findAll(
                DbsnpSubmittedVariantOperationEntity.class);
        if (expectedTotalOperations != totalOperationEntities.size()) {
            fail("Expected " + expectedTotalOperations + " submitted variant operations, there are "
                         + totalOperationEntities.size() + ".\nFull collection content: " + totalOperationEntities);
        }

        List<DbsnpSubmittedVariantOperationEntity> operationEntities = mongoTemplate.find(
                query(where(MERGE_INTO_KEY).is(mergedInto.getAccession()).and("eventType").is(EventType.MERGED)),
                DbsnpSubmittedVariantOperationEntity.class);
        if (expectedMatchingOperations != operationEntities.size()) {
            fail("Expected " + expectedMatchingOperations + " submitted variant operations, there are "
                         + operationEntities.size() + ".\nFull collection content: " + totalOperationEntities);
        }

        operationEntities.forEach(operation -> {
            assertEquals(EventType.MERGED, operation.getEventType());
            List<DbsnpSubmittedVariantInactiveEntity> inactiveObjects = operation.getInactiveObjects();
            assertEquals(1, inactiveObjects.size());
            DbsnpSubmittedVariantInactiveEntity inactiveEntity = inactiveObjects.get(0);
            assertNotEquals(mergedInto.getAccession(), inactiveEntity.getAccession());

            assertEquals(mergedInto.getReferenceSequenceAccession(),
                         inactiveEntity.getReferenceSequenceAccession());
            assertEquals(mergedInto.getTaxonomyAccession(), inactiveEntity.getTaxonomyAccession());
            assertEquals(mergedInto.getProjectAccession(), inactiveEntity.getProjectAccession());
            assertEquals(mergedInto.getContig(), inactiveEntity.getContig());
            assertEquals(mergedInto.getStart(), inactiveEntity.getStart());
            assertEquals(mergedInto.getReferenceAllele(), inactiveEntity.getReferenceAllele());
            assertEquals(mergedInto.getAlternateAllele(), inactiveEntity.getAlternateAllele());
            assertEquals(mergedInto.getClusteredVariantAccession(),
                         inactiveEntity.getClusteredVariantAccession());
            assertEquals(mergedInto.isSupportedByEvidence(),
                         inactiveEntity.isSupportedByEvidence());
            assertEquals(mergedInto.isAssemblyMatch(), inactiveEntity.isAssemblyMatch());
            assertEquals(mergedInto.isAllelesMatch(), inactiveEntity.isAllelesMatch());
            assertEquals(mergedInto.isValidated(), inactiveEntity.isValidated());
        });
    }

    public void assertSubmittedVariantsHaveActiveClusteredVariantsAccession(
            Long accession, DbsnpSubmittedVariantEntity... dbsnpSubmittedVariantEntities) {
        for (DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity : dbsnpSubmittedVariantEntities) {
            assertEquals(accession, dbsnpSubmittedVariantEntity.getClusteredVariantAccession());
        }
        long accessionCount = mongoTemplate.count(query(where(ACCESSION_KEY).is(accession)),
                                              DbsnpClusteredVariantEntity.class);
        assertTrue(accessionCount >= 1);
    }

    public void assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(
            int expectedTotalOperations, int expectedOperations, Long expectedClusteredVariantAccession) {
        int totalCount = mongoTemplate.find(new Query(), DbsnpSubmittedVariantOperationEntity.class).size();
        assertEquals(expectedTotalOperations, totalCount);

        List<DbsnpSubmittedVariantOperationEntity> submittedOperations = mongoTemplate.find(
                query(where(INACTIVE_OBJECTS_RS_KEY).is(expectedClusteredVariantAccession)
                                                    .and(EVENT_TYPE_KEY).is(EventType.UPDATED)),
                DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedOperations, submittedOperations.size());
    }

    public void assertSubmittedOperationType(EventType operationType, long expectedCount) {
        List<DbsnpSubmittedVariantOperationEntity> submittedOperations = mongoTemplate.find(
                query(where(EVENT_TYPE_KEY).is(operationType.toString())), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedCount, submittedOperations.size());
    }

}