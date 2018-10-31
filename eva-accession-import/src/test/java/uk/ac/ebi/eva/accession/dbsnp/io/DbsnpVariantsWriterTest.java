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

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;
import static uk.ac.ebi.eva.accession.dbsnp.io.DbsnpClusteredVariantDeclusteredWriter.DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.CLUSTERED_VARIANT_ACCESSION_1;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.CLUSTERED_VARIANT_ACCESSION_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.CLUSTERED_VARIANT_ACCESSION_3;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.PROJECT_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.START_1;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.START_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.TAXONOMY_1;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.TAXONOMY_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildClusteredVariant;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildClusteredVariantEntity;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildSimpleWrapper;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildSubmittedVariant;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildSubmittedVariantEntity;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.defaultClusteredVariant;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.defaultSubmittedVariant;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
public class DbsnpVariantsWriterTest {

    private static final Long SUBMITTED_VARIANT_ACCESSION_1 = 15L;

    private static final Long SUBMITTED_VARIANT_ACCESSION_2 = 16L;

    private static final Long SUBMITTED_VARIANT_ACCESSION_3 = 17L;

    private DbsnpVariantsWriter dbsnpVariantsWriter;

    private Function<IClusteredVariant, String> hashingFunctionClustered;

    @Autowired
    private MongoTemplate mongoTemplate;

    private ImportCounts importCounts;

    @Autowired
    private DbsnpSubmittedVariantOperationRepository operationRepository;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository;

    @Autowired
    private DbsnpClusteredVariantOperationRepository clusteredOperationRepository;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository;

    @Before
    public void setUp() throws Exception {
        importCounts = new ImportCounts();
        dbsnpVariantsWriter = new DbsnpVariantsWriter(mongoTemplate, operationRepository, submittedVariantRepository,
                                                      clusteredOperationRepository, clusteredVariantRepository,
                                                      importCounts);
        hashingFunctionClustered = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantOperationEntity.class);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
    }

    @Test
    public void writeBasicVariant() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertClusteredVariantStored(1, wrapper);
        assertClusteredVariantDeclusteredStored(0);
    }


    private void assertSubmittedVariantsStored(int expectedVariants, DbsnpSubmittedVariantEntity... submittedVariants) {
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

    private void assertClusteredVariantStored(int expectedVariants, DbsnpVariantsWrapper... wrappers) {
        List<DbsnpClusteredVariantEntity> rsEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(expectedVariants, rsEntities.size());
        assertEquals(expectedVariants, wrappers.length);
        assertEquals(expectedVariants, importCounts.getClusteredVariantsWritten());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(rsEntities.contains(wrappers[i].getClusteredVariant()));
        }
    }

    @Test
    public void writeComplexVariant() throws Exception {
        SubmittedVariant submittedVariant1 = defaultSubmittedVariant();
        SubmittedVariant submittedVariant2 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                  "reference", "alternate_2",
                                                                  CLUSTERED_VARIANT_ACCESSION_1,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED, null);

        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 = buildSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, submittedVariant1);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity2 = buildSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, submittedVariant2);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(
                Arrays.asList(dbsnpSubmittedVariantEntity1, dbsnpSubmittedVariantEntity2));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertSubmittedVariantsStored(2, dbsnpSubmittedVariantEntity1, dbsnpSubmittedVariantEntity2);
        assertClusteredVariantStored(1, wrapper);
    }

    @Test
    public void declusterVariantWithMismatchingAlleles() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                 "reference", "alternate",
                                                                 CLUSTERED_VARIANT_ACCESSION_1,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 = buildSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, submittedVariant);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        dbsnpSubmittedVariantEntity1 = new SubmittedVariantDeclusterProcessor().decluster(dbsnpSubmittedVariantEntity1,
                                                                                          operations,
                                                                                          new ArrayList<>());

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(dbsnpSubmittedVariantEntity1));

        DbsnpSubmittedVariantOperationEntity operationEntity = operations.get(0);
        wrapper.setOperations(Collections.singletonList(operationEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        assertSubmittedVariantDeclusteredStored(wrapper);
        assertClusteredVariantStored(0);
        assertDeclusteringHistoryStored(wrapper.getClusteredVariant().getAccession(),
                                        wrapper.getSubmittedVariants().get(0));
        assertClusteredVariantDeclusteredStored(1, wrapper);
    }

    private DbsnpSubmittedVariantOperationEntity createOperation(SubmittedVariant submittedVariant1) {
        DbsnpSubmittedVariantOperationEntity operationEntity = new DbsnpSubmittedVariantOperationEntity();
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity = buildSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, submittedVariant1);
        DbsnpSubmittedVariantInactiveEntity dbsnpSubmittedVariantInactiveEntity =
                new DbsnpSubmittedVariantInactiveEntity(dbsnpSubmittedVariantEntity);
        operationEntity.fill(EventType.UPDATED, SUBMITTED_VARIANT_ACCESSION_1, null, "Declustered",
                             Collections.singletonList(dbsnpSubmittedVariantInactiveEntity));
        return operationEntity;
    }

    private void assertSubmittedVariantDeclusteredStored(DbsnpVariantsWrapper wrapper) {
        List<DbsnpSubmittedVariantEntity> ssEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(1, ssEntities.size());
        assertEquals(1, wrapper.getSubmittedVariants().size());
        assertEquals(wrapper.getSubmittedVariants().get(0), ssEntities.get(0));
        assertNull(ssEntities.get(0).getClusteredVariantAccession());
        assertEquals(1, importCounts.getSubmittedVariantsWritten());
    }

    private void assertDeclusteringHistoryStored(Long clusteredVariantAccession,
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

    private void assertClusteredVariantDeclusteredStored(int expectedVariants, DbsnpVariantsWrapper... wrappers) {
        List<DbsnpClusteredVariantEntity> rsDeclusteredEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertEquals(expectedVariants, rsDeclusteredEntities.size());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(rsDeclusteredEntities.contains(wrappers[i].getClusteredVariant()));
        }
    }

    @Test
    public void repeatedClusteredVariants() throws Exception {
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate",
                                                                  CLUSTERED_VARIANT_ACCESSION_1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(1, wrapper1);
    }

    @Test
    public void repeatedClusteredVariantsPartiallyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(1, wrapper1);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void mergedClusteredVariantsPartiallyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_1);
        submittedVariant1.setAllelesMatch(allelesMatch);
        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_1, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));
        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        submittedVariant1.setClusteredVariantAccession(null);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant3);
        ClusteredVariant clusteredVariant2 = defaultClusteredVariant();
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                         clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));
        wrapper2.setClusteredVariant(clusteredVariantEntity2);

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));

        assertClusteredVariantStored(1, wrapper1);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
        assertClusteredVariantMergeOperationStored(1, 1, wrapper1.getClusteredVariant());
        assertEquals(CLUSTERED_VARIANT_ACCESSION_1, submittedVariantEntity3.getClusteredVariantAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity1, submittedVariantEntity3);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(2, 1, CLUSTERED_VARIANT_ACCESSION_1);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(2, 1, CLUSTERED_VARIANT_ACCESSION_2);
    }

    @Test
    public void repeatedClusteredVariantsCompletelyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));
        wrapper2.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(0);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void multiallelicClusteredVariantsPartiallyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity = createOperation(submittedVariant1);
        wrapper.setOperations(Collections.singletonList(operationEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertClusteredVariantStored(1, wrapper);
        assertClusteredVariantDeclusteredStored(1, wrapper);
    }

    @Test
    public void multiallelicClusteredVariantsCompletelyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);

        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper1));
        assertClusteredVariantStored(0);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void mergeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // Try to write wrapper twice, the second time it will be considered a duplicate and ignored
        // wrapper2 will be merged into the previous accession
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, 1, submittedVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private void assertSubmittedVariantMergeOperationStored(int expectedTotalOperations, int expectedMatchingOperations,
                                                            DbsnpSubmittedVariantEntity mergedInto) {
        List<DbsnpSubmittedVariantOperationEntity> operationEntities = mongoTemplate.findAll(
                DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedTotalOperations, operationEntities.size());

        long matchingOperations = operationEntities
                .stream()
                .filter(op -> mergedInto.getAccession().equals(op.getMergedInto()))
                .map(operation -> {
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
                    return 1;
                })
                .count();
        assertEquals(expectedMatchingOperations, matchingOperations);
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // wrapper3 should not issue another identical merge event
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, 1, submittedVariantEntity);
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity2, submittedVariantEntity2));

        // should not issue another identical merge event for the second variant in wrapper2
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, 1, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2));

        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_3,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(2, 2, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_3,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity2, submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(2, 2, submittedVariantEntity);
    }


    @Test
    public void mergeDuplicateClusteredVariantsInTheSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                                         clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    expectedSubmittedVariantEntity2);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(1, 1, clusteredVariantEntity2.getAccession());
        assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private DbsnpSubmittedVariantEntity changeRS(DbsnpSubmittedVariantEntity submittedVariant, Long mergedInto) {
        // Need to create a new one because DbsnpSubmittedVariantEntity has no setters
        SubmittedVariant variant = new SubmittedVariant(submittedVariant);
        variant.setClusteredVariantAccession(mergedInto);

        Long accession = submittedVariant.getAccession();
        String hash = submittedVariant.getHashedMessage();
        int version = submittedVariant.getVersion();
        return new DbsnpSubmittedVariantEntity(accession, hash, variant, version);
    }

    private void assertSubmittedVariantsHaveActiveClusteredVariantsAccession(
            Long accession, DbsnpSubmittedVariantEntity... dbsnpSubmittedVariantEntities) {
        for (DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity : dbsnpSubmittedVariantEntities) {
            assertEquals(accession, dbsnpSubmittedVariantEntity.getClusteredVariantAccession());
        }
    }

    private void assertSubmittedUpdateOperationsHaveClusteredVariantAccession(int totalExpectedCount, int expectedCount,
                                                                              Long expectedClusteredVariantAccession) {
        int totalCount = mongoTemplate.find(new Query(), DbsnpSubmittedVariantOperationEntity.class).size();
        assertEquals(totalExpectedCount, totalCount);

        List<DbsnpSubmittedVariantOperationEntity> submittedOperations = mongoTemplate.find(
                query(where("inactiveObjects.rs").is(expectedClusteredVariantAccession)
                                                 .and("eventType").is(EventType.UPDATED)),
                DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedCount, submittedOperations.size());
    }

    @Test
    public void mergeDuplicateClusteredVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                                         clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    expectedSubmittedVariantEntity2);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(1, 1, clusteredVariantEntity2.getAccession());
        assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    @Test
    public void mergeThreeDuplicateClusteredVariantsInSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                                         clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        Long clusteredVariantAccession3 = CLUSTERED_VARIANT_ACCESSION_3;
        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant3);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_3,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    wrapper2.getSubmittedVariants().get(0),
                                                                    wrapper3.getSubmittedVariants().get(0));
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(2, 1, clusteredVariantEntity2.getAccession());
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(2, 1, clusteredVariantEntity3.getAccession());

        assertClusteredVariantMergeOperationStored(2, 2, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private void assertClusteredVariantMergeOperationStored(int expectedTotalOperations, int expectedMatchingOperations,
                                                            DbsnpClusteredVariantEntity mergedInto) {
        List<DbsnpClusteredVariantOperationEntity> operationEntities = mongoTemplate.find(
                new Query(), DbsnpClusteredVariantOperationEntity.class);
        assertEquals(expectedTotalOperations, operationEntities.size());

        long matchingOperations = operationEntities
                .stream()
                .filter(op -> op.getMergedInto().equals(mergedInto.getAccession()))
                .map(operation -> {
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
                    return 1;
                })
                .count();
        assertEquals(expectedMatchingOperations, matchingOperations);
    }

    /**
     * The test data is not real, but this exact thing happened with rs193927678.
     * <p>
     * rs193927678 is mapped in 2 positions, so corresponds to 2 clustered variants with different hash and same RS
     * accession. This results in 2 submitted variants with different hash and same SS accession. Each entry of this
     * RS also makes hash collision with rs1095750933 and rs347458720.
     * <p>
     * So to decide which active RS should we link in each SS, we have to take into account the hash as well. One SS
     * will be linked to rs1095750933 and the other to rs347458720.
     * <p>
     * Note that there will be 2 clustered variant operations, as there are 2 hashes for rs193927678. Each operation
     * is a merge into rs1095750933 and rs347458720. Moreover, there are no declustered clustered variant operations.
     */
    @Test
    public void mergeRs193927678() throws Exception {
        Long clusteredVariantAccession1 = 347458720L;
        Long clusteredVariantAccession2 = 1095750933L;
        Long clusteredVariantAccession3 = 193927678L;
        Long submittedVariantAccession1 = 2688593462L;
        Long submittedVariantAccession2 = 2688600186L;
        Long submittedVariantAccession3 = 252447620L;

        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                         submittedVariant);
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                         clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        ClusteredVariant clusteredVariant2 = buildClusteredVariant(START_2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3);
        submittedVariant3.setProjectAccession(PROJECT_2);
        SubmittedVariant submittedVariant4 = defaultSubmittedVariant();
        submittedVariant4.setStart(START_2);
        submittedVariant4.setProjectAccession(PROJECT_2);
        submittedVariant4.setClusteredVariantAccession(clusteredVariantAccession3);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant3);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant4);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariantEntity),
                clusteredVariantEntity);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = new DbsnpClusteredVariantEntity(
                clusteredVariantAccession3, hashingFunctionClustered.apply(clusteredVariantEntity2),
                clusteredVariantEntity2);
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3, wrapper4));

        assertClusteredVariantStored(2, wrapper, wrapper2);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3,
                                                                               clusteredVariantEntity.getAccession());
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity4 = changeRS(submittedVariantEntity4,
                                                                               clusteredVariantEntity2.getAccession());
        assertSubmittedVariantsStored(4, submittedVariantEntity, submittedVariantEntity2,
                                      expectedSubmittedVariantEntity3, expectedSubmittedVariantEntity4);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    expectedSubmittedVariantEntity3);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper2.getClusteredVariant().getAccession(),
                                                                    expectedSubmittedVariantEntity4);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(2, 2, clusteredVariantAccession3);
        assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity);
        assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity2);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    /**
     * The test data is not real, but this exact thing happened with rs638662487.
     * <p>
     * rs638662487 should be declustered from ss1387800177 because one orientation is unknown. This is tracked writing
     * the RS in dbsnpClusteredVariantEntityDeclustered instead of dbsnpClusteredVariantEntity.
     * <p>
     * However, another equivalent RS (rs268262202) was already declustered, and as this case is a collision of same
     * hash but different accession, it should be written in the clustered operations collection as a merge.
     */
    @Test
    public void declusterRs638662487() throws Exception {
        Long clusteredVariantAccession1 = 268262202L;
        Long clusteredVariantAccession2 = 638662487L;
        Long submittedVariantAccession1 = 528860089L;
        Long submittedVariantAccession2 = 1387800177L;

        SubmittedVariant submittedVariant = buildSubmittedVariant(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                         submittedVariant);
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                         clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);

        ArrayList<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariant = new SubmittedVariantDeclusterProcessor().decluster(
                submittedVariantEntity, operations, new ArrayList<>());

        wrapper.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariant));
        wrapper.setOperations(operations);


        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2, PROJECT_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        ClusteredVariant clusteredVariant2 = defaultClusteredVariant();
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);

        ArrayList<DbsnpSubmittedVariantOperationEntity> operations2 = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariant2 = new SubmittedVariantDeclusterProcessor().decluster(
                submittedVariantEntity2, operations2, new ArrayList<>());
        wrapper2.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariant2));
        wrapper2.setOperations(operations2);


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));

        assertSubmittedVariantsStored(2, declusteredSubmittedVariant, declusteredSubmittedVariant2);
        assertEquals(1, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
        assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
    }

    /**
     * Clustered variant rs136611820 was merged into several other clustered variants with the same hash: [42568024,
     * 42568025]
     * <p>
     * This happened because rs136611820 had several locations. One matched the location of rs42568024 and other matched
     * the location of rs42568025. This makes it harder to decide which RS should be the active one and what to do with
     * the other RSs.
     * <p>
     * The desired result is that an RS can be merged several times into other RSs if they all have the same hash, but
     * in the main collection only one of those will be present.
     * <p>
     * The real case is more complicated because it involves also declusterings
     */
    @Test
    public void simplifiedRs136611820() throws Exception {
        // given
        Long clusteredVariantAccession1 = 42568024L;
        Long clusteredVariantAccession2 = 42568025L;
        Long clusteredVariantAccession3 = 136611820L;
        Long submittedVariantAccession1 = 64289612L;
        Long submittedVariantAccession2 = 64289614L;
        Long submittedVariantAccession3 = 266911375L;
        Long submittedVariantAccession4 = 266602754L;

        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        SubmittedVariant submittedVariant1 = buildSubmittedVariant(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                          submittedVariant1);
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity1);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        SubmittedVariant submittedVariant4 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant3);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = buildSubmittedVariantEntity(submittedVariantAccession4,
                                                                                          submittedVariant4);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3, wrapper4));

        // then
        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity1.getAccession());
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3,
                                                                               clusteredVariantEntity1.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity1, expectedSubmittedVariantEntity3);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    expectedSubmittedVariantEntity2,
                                                                    expectedSubmittedVariantEntity3);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(5, 1, clusteredVariantAccession2);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(5, 2, clusteredVariantAccession3);
        assertSubmittedVariantMergeOperationStored(5, 1, submittedVariantEntity1);
        assertSubmittedVariantMergeOperationStored(5, 1, submittedVariantEntity3);
        // could be 3 operations total? 2 copies for clusteredVariantEntity3

        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    /**
     * Clustered variant rs136611820 was merged into several other clustered variants with the same hash: [42568024,
     * 42568025]
     * <p>
     * This happened because rs136611820 had several locations. One matched the location of rs42568024 and other matched
     * the location of rs42568025. This makes it harder to decide which RS should be the active one and what to do with
     * the other RSs.
     * <p>
     * The desired result is that an RS can be merged several times into other RSs if they all have the same hash, but
     * in the main collection only one of those will be present.
     * <p>
     * This test is similar to the previous one, but closer to the real case, because this one involves also the
     * declusterings.
     */
    @Test
    public void rs136611820() throws Exception {
        // given
        Long clusteredVariantAccession1 = 42568024L;
        Long clusteredVariantAccession2 = 42568025L;
        Long clusteredVariantAccession3 = 136611820L;
        Long submittedVariantAccession1 = 64289612L;
        Long submittedVariantAccession2 = 64289614L;
        Long submittedVariantAccession3 = 266911375L;
        Long submittedVariantAccession4 = 266911374L;

        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        SubmittedVariant submittedVariant = buildSubmittedVariant(clusteredVariantAccession1, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                         submittedVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                         clusteredVariant);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariant = new SubmittedVariantDeclusterProcessor().decluster(
                submittedVariantEntity, operations, new ArrayList<>());
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariant));
        wrapper.setOperations(operations);

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        SubmittedVariant submittedVariant4 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant3);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations3 = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariantEntity3 =
                new SubmittedVariantDeclusterProcessor().decluster(submittedVariantEntity3, operations3,
                                                                   new ArrayList<>());
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = buildSubmittedVariantEntity(submittedVariantAccession4,
                                                                                          submittedVariant4);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariantEntity3));
        wrapper3.setOperations(operations3);

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3, wrapper4));

        // then
        assertRs136611820(wrapper, wrapper2, wrapper3, wrapper4);
    }

    public void assertRs136611820(DbsnpVariantsWrapper wrapper1, DbsnpVariantsWrapper wrapper2,
                                  DbsnpVariantsWrapper wrapper3, DbsnpVariantsWrapper wrapper4) {
        Long clusteredVariantAccession1 = wrapper1.getClusteredVariant().getAccession();
        Long clusteredVariantAccession2 = wrapper2.getClusteredVariant().getAccession();
        Long clusteredVariantAccession3 = wrapper3.getClusteredVariant().getAccession();
        DbsnpSubmittedVariantEntity submittedVariantEntity = wrapper1.getSubmittedVariants().get(0);
        DbsnpClusteredVariantEntity clusteredVariantEntity = wrapper1.getClusteredVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = wrapper2.getSubmittedVariants().get(0);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = wrapper2.getClusteredVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = wrapper3.getSubmittedVariants().get(0);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = wrapper4.getSubmittedVariants().get(0);

        assertClusteredVariantStored(1, wrapper1);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity1 = changeRS(submittedVariantEntity, null);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3, null);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity4 = changeRS(submittedVariantEntity4,
                                                                               clusteredVariantAccession2);
        assertSubmittedVariantsStored(4, expectedSubmittedVariantEntity1, expectedSubmittedVariantEntity2,
                                      expectedSubmittedVariantEntity3, expectedSubmittedVariantEntity4);
        assertSubmittedVariantsHaveActiveClusteredVariantsAccession(clusteredVariantAccession2,
                                                                    expectedSubmittedVariantEntity2,
                                                                    expectedSubmittedVariantEntity4);
        assertSubmittedOperationType(EventType.UPDATED, 3L);
        assertSubmittedOperationType(EventType.MERGED, 0L);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(3, 1, clusteredVariantAccession1);
        assertSubmittedUpdateOperationsHaveClusteredVariantAccession(3, 2, clusteredVariantAccession3);

        // the first non-declustered RS is clusteredVariantEntity2. The non-declustered clusteredVariantEntity4 is
        // merged into it. Then the declustered clusteredVariantEntity4 is merged into the previously declustered
        // clusteredVariantEntity1. Then, we have clusteredVariantEntity4 merged into 2 RS, and we choose the
        // non-declustered one as the only active one, so we merge clusteredVariantEntity1 into clusteredVariantEntity2
        assertClusteredVariantMergeOperationStored(3, 2, clusteredVariantEntity2);  // the first non declustered RS,
        assertClusteredVariantMergeOperationStored(3, 1, clusteredVariantEntity);

        List<DbsnpClusteredVariantEntity> declustered = mongoTemplate.findAll(
                DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertEquals(1, declustered.size());
        assertEquals(clusteredVariantAccession1, declustered.get(0).getAccession());
    }

    private void assertSubmittedOperationType(EventType operationType, long expectedCount) {
        List<DbsnpSubmittedVariantOperationEntity> submittedOperations = mongoTemplate.find(
                query(where("eventType").is(operationType.toString())), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedCount, submittedOperations.size());
    }

    @Test
    public void multiChunkRs136611820() throws Exception {
        // given
        Long clusteredVariantAccession1 = 42568024L;
        Long clusteredVariantAccession2 = 42568025L;
        Long clusteredVariantAccession3 = 136611820L;
        Long submittedVariantAccession1 = 64289612L;
        Long submittedVariantAccession2 = 64289614L;
        Long submittedVariantAccession3 = 266911375L;
        Long submittedVariantAccession4 = 266911374L;

        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        SubmittedVariant submittedVariant = buildSubmittedVariant(clusteredVariantAccession1, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                         submittedVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                         clusteredVariant);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariant = new SubmittedVariantDeclusterProcessor().decluster(
                submittedVariantEntity, operations, new ArrayList<>());
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariant));
        wrapper.setOperations(operations);

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        SubmittedVariant submittedVariant4 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant3);
        ArrayList<DbsnpSubmittedVariantOperationEntity> operations3 = new ArrayList<>();
        DbsnpSubmittedVariantEntity declusteredSubmittedVariantEntity3 =
                new SubmittedVariantDeclusterProcessor().decluster(submittedVariantEntity3, operations3,
                                                                   new ArrayList<>());
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = buildSubmittedVariantEntity(submittedVariantAccession4,
                                                                                          submittedVariant4);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariantEntity3));
        wrapper3.setOperations(operations3);

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper3));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper4));

        // then
        assertRs136611820(wrapper, wrapper2, wrapper3, wrapper4);
    }

}
