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
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
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
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;
import static uk.ac.ebi.eva.accession.dbsnp.io.DbsnpClusteredVariantDeclusteredWriter
        .DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
public class DbsnpVariantsWriterTest {

    private static final int TAXONOMY_1 = 3880;

    private static final int TAXONOMY_2 = 3882;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final int START_1 = 100;

    private static final int START_2 = 200;

    private static final Long CLUSTERED_VARIANT_ACCESSION_1 = 12L;

    private static final Long CLUSTERED_VARIANT_ACCESSION_2 = 13L;

    private static final Long CLUSTERED_VARIANT_ACCESSION_3 = 14L;

    private static final VariantType VARIANT_TYPE = VariantType.SNV;

    private static final Long SUBMITTED_VARIANT_ACCESSION_1 = 15L;

    private static final Long SUBMITTED_VARIANT_ACCESSION_2 = 16L;

    private static final Long SUBMITTED_VARIANT_ACCESSION_3 = 17L;

    private DbsnpVariantsWriter dbsnpVariantsWriter;

    private Function<ISubmittedVariant, String> hashingFunctionSubmitted;

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
        hashingFunctionSubmitted = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
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
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertClusteredVariantStored(1, wrapper);
        assertClusteredVariantDeclusteredStored(0);
    }

    private SubmittedVariant defaultSubmittedVariant() {
        return new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                    "reference", "alternate", CLUSTERED_VARIANT_ACCESSION_1,
                                    DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                    DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED, null);
    }

    private DbsnpVariantsWrapper buildSimpleWrapper(List<DbsnpSubmittedVariantEntity> submittedVariantEntities) {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED, null);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setSubmittedVariants(submittedVariantEntities);
        wrapper.setClusteredVariant(clusteredVariantEntity);
        return wrapper;
    }

    private void assertSubmittedVariantsStored(int expectedVariants, DbsnpSubmittedVariantEntity... submittedVariants) {
        List<DbsnpSubmittedVariantEntity> ssEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(expectedVariants, ssEntities.size());
        assertEquals(expectedVariants, submittedVariants.length);
        assertEquals(expectedVariants, importCounts.getSubmittedVariantsWritten());
        for (int i = 0; i < expectedVariants; i++) {
            assertTrue(ssEntities.contains(submittedVariants[i]));
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

        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                hashingFunctionSubmitted.apply(submittedVariant1),
                                                submittedVariant1, 1);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity2 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                hashingFunctionSubmitted.apply(submittedVariant2),
                                                submittedVariant2, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Arrays.asList(dbsnpSubmittedVariantEntity1,
                                                                        dbsnpSubmittedVariantEntity2));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertSubmittedVariantsStored(2, dbsnpSubmittedVariantEntity1, dbsnpSubmittedVariantEntity2);
        assertClusteredVariantStored(1, wrapper);
    }

    @Test
    public void declusterVariantWithMismatchingAlleles() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                 "reference", "alternate",
                                                                 EXPECTED_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                hashingFunctionSubmitted.apply(submittedVariant),
                                                submittedVariant, 1);
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
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
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
        assertEquals(clusteredVariantAccession,
                     operationEntities.get(0).getInactiveObjects().get(0).getClusteredVariantAccession());
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
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
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
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(1, wrapper1);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void repeatedClusteredVariantsCompletelyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
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
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Arrays.asList(submittedVariantEntity1,
                                                                        submittedVariantEntity2));

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
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);

        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity1,
                                                                         submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper1));
        assertClusteredVariantStored(0);
        assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void mergeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // Try to write wrapper twice, the second time it will be considered a duplicate and ignored
        // wrapper2 will be merged into the previous accession
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, submittedVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private void assertSubmittedVariantMergeOperationStored(int expectedOperations,
                                                            DbsnpSubmittedVariantEntity submittedVariantEntity) {
        List<DbsnpSubmittedVariantOperationEntity> operationEntities = mongoTemplate.find(
                new Query(), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedOperations, operationEntities.size());
        operationEntities
                .stream()
                .filter(op -> op.getMergedInto().equals(submittedVariantEntity.getAccession()))
                .forEach(operation -> {
                    assertEquals(EventType.MERGED, operation.getEventType());
                    List<DbsnpSubmittedVariantInactiveEntity> inactiveObjects = operation.getInactiveObjects();
                    assertEquals(1, inactiveObjects.size());
                    DbsnpSubmittedVariantInactiveEntity inactiveEntity = inactiveObjects.get(0);
                    assertNotEquals(submittedVariantEntity.getAccession(), inactiveEntity.getAccession());

                    assertEquals(submittedVariantEntity.getReferenceSequenceAccession(),
                                 inactiveEntity.getReferenceSequenceAccession());
                    assertEquals(submittedVariantEntity.getTaxonomyAccession(), inactiveEntity.getTaxonomyAccession());
                    assertEquals(submittedVariantEntity.getProjectAccession(), inactiveEntity.getProjectAccession());
                    assertEquals(submittedVariantEntity.getContig(), inactiveEntity.getContig());
                    assertEquals(submittedVariantEntity.getStart(), inactiveEntity.getStart());
                    assertEquals(submittedVariantEntity.getReferenceAllele(), inactiveEntity.getReferenceAllele());
                    assertEquals(submittedVariantEntity.getAlternateAllele(), inactiveEntity.getAlternateAllele());
                    assertEquals(submittedVariantEntity.getClusteredVariantAccession(),
                                 inactiveEntity.getClusteredVariantAccession());
                    assertEquals(submittedVariantEntity.isSupportedByEvidence(), inactiveEntity.isSupportedByEvidence());
                    assertEquals(submittedVariantEntity.isAssemblyMatch(), inactiveEntity.isAssemblyMatch());
                    assertEquals(submittedVariantEntity.isAllelesMatch(), inactiveEntity.isAllelesMatch());
                    assertEquals(submittedVariantEntity.isValidated(), inactiveEntity.isValidated());
                });
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // wrapper3 should not issue another identical merge event
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, submittedVariantEntity);
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2,
                                                                         submittedVariantEntity2));

        // should not issue another identical merge event for the second variant in wrapper2
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(1, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2));

        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_3, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(2, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_3, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2,
                                                                         submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantMergeOperationStored(2, submittedVariantEntity);
    }


    @Test
    public void mergeDuplicateClusteredVariantsInTheSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1, VARIANT_TYPE,
                                                                 DEFAULT_VALIDATED, null);

        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_2, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertSubmittedVariantsHasActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                   wrapper.getSubmittedVariants().get(0),
                                                                   expectedSubmittedVariantEntity2);
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));
        assertClusteredVariantMergeOperationStored(1, clusteredVariantEntity);
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

    private void assertSubmittedVariantsHasActiveClusteredVariantsAccession(
            Long accession,
            DbsnpSubmittedVariantEntity... dbsnpSubmittedVariantEntities) {
        for (DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity : dbsnpSubmittedVariantEntities) {
            assertEquals(accession, dbsnpSubmittedVariantEntity.getClusteredVariantAccession());
        }
    }

    @Test
    public void mergeDuplicateClusteredVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1, VARIANT_TYPE,
                                                                 DEFAULT_VALIDATED, null);

        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertSubmittedVariantsHasActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                   wrapper.getSubmittedVariants().get(0),
                                                                   expectedSubmittedVariantEntity2);
        assertClusteredVariantMergeOperationStored(1, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    @Test
    public void mergeThreeDuplicateClusteredVariantsInSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1, VARIANT_TYPE,
                                                                 DEFAULT_VALIDATED, null);

        DbsnpClusteredVariantEntity clusteredVariantEntity = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_1, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        DbsnpClusteredVariantEntity clusteredVariantEntity2 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_2, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        DbsnpClusteredVariantEntity clusteredVariantEntity3 = new DbsnpClusteredVariantEntity(
                CLUSTERED_VARIANT_ACCESSION_3, hashingFunctionClustered.apply(clusteredVariant), clusteredVariant);
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3));

        assertClusteredVariantStored(1, wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertSubmittedVariantsHasActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                   wrapper.getSubmittedVariants().get(0),
                                                                   wrapper2.getSubmittedVariants().get(0),
                                                                   wrapper3.getSubmittedVariants().get(0));
        assertClusteredVariantMergeOperationStored(2, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private void assertClusteredVariantMergeOperationStored(int expectedOperations,
                                                            DbsnpClusteredVariantEntity clusteredVariantEntity) {
        List<DbsnpClusteredVariantOperationEntity> operationEntities = mongoTemplate.find(
                new Query(), DbsnpClusteredVariantOperationEntity.class);
        assertEquals(expectedOperations, operationEntities.size());
        operationEntities
                .stream()
                .filter(op -> op.getMergedInto().equals(clusteredVariantEntity.getAccession()))
                .forEach(operation -> {
                    assertEquals(EventType.MERGED, operation.getEventType());
                    List<DbsnpClusteredVariantInactiveEntity> inactiveObjects = operation.getInactiveObjects();
                    assertEquals(1, inactiveObjects.size());
                    DbsnpClusteredVariantInactiveEntity inactiveEntity = inactiveObjects.get(0);
                    assertNotEquals(clusteredVariantEntity.getAccession(), inactiveEntity.getAccession());

                    assertEquals(clusteredVariantEntity.getAssemblyAccession(),
                                 inactiveEntity.getAssemblyAccession());
                    assertEquals(clusteredVariantEntity.getContig(), inactiveEntity.getContig());
                    assertEquals(clusteredVariantEntity.getStart(), inactiveEntity.getStart());
                    assertEquals(clusteredVariantEntity.getTaxonomyAccession(),
                                 inactiveEntity.getTaxonomyAccession());
                    assertEquals(clusteredVariantEntity.getType(), inactiveEntity.getType());
                    assertEquals(clusteredVariantEntity.isValidated(), inactiveEntity.isValidated());
                });
    }
}
