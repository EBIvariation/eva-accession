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
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
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

    private static final int START = 100;

    private static final Long CLUSTERED_VARIANT_ACCESSION = 12L;

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

    @Before
    public void setUp() throws Exception {
        importCounts = new ImportCounts();
        dbsnpVariantsWriter = new DbsnpVariantsWriter(mongoTemplate, operationRepository, submittedVariantRepository,
                                                      importCounts);
        hashingFunctionSubmitted = new DbsnpSubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        hashingFunctionClustered = new DbsnpClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
    }

    @Test
    public void writeBasicVariant() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                 "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant), submittedVariant, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertClusteredVariantStored(wrapper);
    }

    private DbsnpVariantsWrapper buildSimpleWrapper(List<DbsnpSubmittedVariantEntity> submittedVariantEntities) {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START,
                                                                 VARIANT_TYPE, DEFAULT_VALIDATED);
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

    private void assertClusteredVariantStored(DbsnpVariantsWrapper wrapper) {
        List<DbsnpClusteredVariantEntity> rsEntities = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(1, rsEntities.size());
        assertEquals(wrapper.getClusteredVariant(), rsEntities.get(0));
        assertEquals(1, importCounts.getClusteredVariantsWritten());
    }

    @Test
    public void writeComplexVariant() throws Exception {
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                  "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
        SubmittedVariant submittedVariant2 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                  "reference", "alternate_2",
                                                                  CLUSTERED_VARIANT_ACCESSION,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);

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
        assertClusteredVariantStored(wrapper);
    }

    @Test
    public void declusterVariantWithMismatchingAlleles() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                 "reference", "alternate", null,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 allelesMatch, DEFAULT_VALIDATED);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 =
                new DbsnpSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                hashingFunctionSubmitted.apply(submittedVariant),
                                                submittedVariant, 1);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(dbsnpSubmittedVariantEntity1));

        DbsnpSubmittedVariantOperationEntity operationEntity = createOperation(submittedVariant);
        wrapper.setOperations(Collections.singletonList(operationEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        assertSubmittedVariantDeclusteredStored(wrapper);
        assertClusteredVariantStored(wrapper);
        assertDeclusteringHistoryStored(wrapper);
        assertClusterVariantDeclusteredStored(wrapper);
    }

    private DbsnpSubmittedVariantOperationEntity createOperation(SubmittedVariant submittedVariant1) {
        DbsnpSubmittedVariantOperationEntity operationEntity = new DbsnpSubmittedVariantOperationEntity();
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity = new DbsnpSubmittedVariantEntity
                (SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpSubmittedVariantInactiveEntity dbsnpSubmittedVariantInactiveEntity =
                new DbsnpSubmittedVariantInactiveEntity(dbsnpSubmittedVariantEntity);
        operationEntity.fill(EventType.UPDATED, SUBMITTED_VARIANT_ACCESSION_1, null,
                             SubmittedVariantDeclusterProcessor.DECLUSTERED,
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

    private void assertDeclusteringHistoryStored(DbsnpVariantsWrapper wrapper) {
        List<DbsnpSubmittedVariantEntity> ssEntities =
                mongoTemplate.find(new Query(), DbsnpSubmittedVariantEntity.class);
        List<DbsnpSubmittedVariantOperationEntity> operationEntities =
                mongoTemplate.find(new Query(), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(1, operationEntities.size());
        assertEquals(EventType.UPDATED, operationEntities.get(0).getEventType());
        assertEquals(1, operationEntities.get(0).getInactiveObjects().size());
        assertEquals(wrapper.getSubmittedVariants().get(0).getClusteredVariantAccession(),
                     operationEntities.get(0).getInactiveObjects().get(0).getClusteredVariantAccession());
        assertEquals(ssEntities.get(0).getAccession(), operationEntities.get(0).getAccession());
        assertEquals(1, importCounts.getOperationsWritten());
    }

    private void assertClusterVariantDeclusteredStored(DbsnpVariantsWrapper wrapper) {
        List<DbsnpClusteredVariantEntity> rsDeclusteredEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertEquals(1, rsDeclusteredEntities.size());
        assertEquals(wrapper.getClusteredVariant(), rsDeclusteredEntities.get(0));
    }

    @Test
    public void repeatedClusteredVariants() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START,
                                                                  "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                  "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(wrapper1);
    }

    @Test
    public void repeatedClusteredVariantsDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START,
                                                                  "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                  "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = new DbsnpSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, hashingFunctionSubmitted.apply(submittedVariant2), submittedVariant2, 1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        DbsnpSubmittedVariantOperationEntity operationEntity1 = createOperation(submittedVariant1);
        wrapper1.setOperations(Collections.singletonList(operationEntity1));
        DbsnpSubmittedVariantOperationEntity operationEntity2 = createOperation(submittedVariant2);
        wrapper2.setOperations(Collections.singletonList(operationEntity2));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertClusteredVariantStored(wrapper1);
        assertClusterVariantDeclusteredStored(wrapper1);
    }

    @Test
    public void mergeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                 "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
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

        assertClusteredVariantStored(wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertMergeOperationStored(1);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private void assertMergeOperationStored(int expectedOperations) {
        List<DbsnpSubmittedVariantOperationEntity> operationEntities = mongoTemplate.find(
                new Query(), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(expectedOperations, operationEntities.size());
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                 "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
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

        assertClusteredVariantStored(wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertMergeOperationStored(1);
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                 "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
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

        assertClusteredVariantStored(wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertMergeOperationStored(1);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                 "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
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

        assertClusteredVariantStored(wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertMergeOperationStored(2);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START,
                                                                 "reference", "alternate", CLUSTERED_VARIANT_ACCESSION,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED);
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

        assertClusteredVariantStored(wrapper);
        assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertMergeOperationStored(2);
    }

}
