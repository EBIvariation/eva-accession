/*
 * Copyright 2024 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_STEP;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_DUPLICATE_RS_ACC_QC_JOB;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@TestPropertySource("classpath:duplicate-rs-acc-qc-test.properties")
public class DuplicateRSAccQCStepConfigurationTest {

    private static final String TEST_DB = "test-db";
    private static final String duplicateRsAccFile = "src/test/resources/duplicateRSAcc.csv";

    @Autowired
    @Qualifier(JOB_LAUNCHER_DUPLICATE_RS_ACC_QC_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() throws IOException {
        this.mongoClient.dropDatabase(TEST_DB);
        Files.deleteIfExists(Paths.get(duplicateRsAccFile));
    }

    @After
    public void tearDown() throws IOException {
        this.mongoClient.dropDatabase(TEST_DB);
        Files.deleteIfExists(Paths.get(duplicateRsAccFile));
    }

    @Test
    public void contextLoads() {

    }

    @Test
    public void duplicateRSAccQCTest_OnlyOneDocument() throws IOException {
        populateTestDataOnlyOneDocument(this.mongoTemplate);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_RS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertDuplicateRSAccFileIsEmpty();
    }

    @Test
    public void duplicateRSAccQCTest_Duplicate() throws IOException {
        populateTestDataDuplicate(this.mongoTemplate);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_RS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        Set<Long> expectedRSAccs = new HashSet<>();
        expectedRSAccs.add(2L);
        assertDuplicateRSAccFileContains(expectedRSAccs);
    }

    @Test
    public void duplicateRSAccQCTest_NoDuplicate_1() throws IOException {
        populateTestDataNoDuplicate1(this.mongoTemplate);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_RS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertDuplicateRSAccFileIsEmpty();
    }

    @Test
    public void duplicateRSAccQCTest_NoDuplicate_2() throws IOException {
        populateTestDataNoDuplicate2(this.mongoTemplate);
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_RS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertDuplicateRSAccFileIsEmpty();
    }

    public static void populateTestDataOnlyOneDocument(MongoTemplate mongoTemplate) {
        SubmittedVariantEntity ss1 = createSS("GCA_000000001.1", 60711, "study1", "hash" + 1, "chr1", 1L, 1L, 100L, "C", "T");
        ClusteredVariantEntity rs1 = createRS(ss1);

        mongoTemplate.save(ss1, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        mongoTemplate.save(rs1, mongoTemplate.getCollectionName(ClusteredVariantEntity.class));
    }

    public static void populateTestDataDuplicate(MongoTemplate mongoTemplate) {
        SubmittedVariantEntity ss11 = createSS("GCA_000000001.1", 60711, "hash" + 11, "study1", "chr1", 11L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss12 = createSS("GCA_000000001.1", 60711, "hash" + 12, "study2", "chr1", 12L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss21 = createSS("GCA_000000001.2", 60711, "hash" + 21, "study1", "chr1", 21L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss22 = createSS("GCA_000000001.2", 60711, "hash" + 22, "study2", "chr1", 22L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss31 = createSS("GCA_000000001.3", 60711, "hash" + 31, "study1", "chr1", 31L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss32 = createSS("GCA_000000001.3", 60711, "hash" + 32, "study2", "chr1", 32L, 2L, 100L, "C", "T");
        ClusteredVariantEntity rs11 = createRS(ss11);
        ClusteredVariantEntity rs21 = createRS(ss21);
        ClusteredVariantEntity rs31 = createRS(ss31);

        mongoTemplate.insert(Arrays.asList(ss11, ss12, ss21, ss22, ss31, ss32), SubmittedVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(rs11, rs21, rs31), ClusteredVariantEntity.class);
    }

    public static void populateTestDataNoDuplicate1(MongoTemplate mongoTemplate) {
        SubmittedVariantEntity ss11 = createSS("GCA_000000001.1", 60711, "hash" + 11, "study1", "chr1", 11L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss12 = createSS("GCA_000000001.1", 60711, "hash" + 12, "study2", "chr1", 12L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss21 = createSS("GCA_000000001.2", 60711, "hash" + 21, "study1", "chr1", 11L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss22 = createSS("GCA_000000001.2", 60711, "hash" + 22, "study2", "chr1", 22L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss31 = createSS("GCA_000000001.3", 60711, "hash" + 31, "study1", "chr1", 31L, 2L, 100L, "C", "T");
        SubmittedVariantEntity ss32 = createSS("GCA_000000001.3", 60711, "hash" + 32, "study2", "chr1", 22L, 2L, 100L, "C", "T");
        ClusteredVariantEntity rs11 = createRS(ss11);
        ClusteredVariantEntity rs21 = createRS(ss21);
        ClusteredVariantEntity rs31 = createRS(ss31);

        mongoTemplate.insert(Arrays.asList(ss11, ss12, ss21, ss22, ss31, ss32), SubmittedVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(rs11, rs21, rs31), ClusteredVariantEntity.class);
    }

    public static void populateTestDataNoDuplicate2(MongoTemplate mongoTemplate) {
        SubmittedVariantEntity ss11 = createSS("GCA_000000001.1", 60711, "hash" + 11, "study1", "chr1", 11L, 3L, 100L, "C", "T");
        SubmittedVariantEntity ss12 = createSS("GCA_000000001.1", 60711, "hash" + 12, "study2", "chr1", 12L, 3L, 100L, "C", "T");
        SubmittedVariantEntity ss21 = createSS("GCA_000000001.2", 60711, "hash" + 21, "study1", "chr1", 21L, 3L, 100L, "C", "T");
        SubmittedVariantEntity ss22 = createSS("GCA_000000001.2", 60711, "hash" + 22, "study2", "chr1", 12L, 3L, 100L, "C", "T");
        SubmittedVariantEntity ss31 = createSS("GCA_000000001.3", 60711, "hash" + 31, "study1", "chr1", 11L, 3L, 100L, "C", "T");
        SubmittedVariantEntity ss32 = createSS("GCA_000000001.3", 60711, "hash" + 32, "study2", "chr1", 32L, 3L, 100L, "C", "T");
        ClusteredVariantEntity rs11 = createRS(ss11);
        ClusteredVariantEntity rs21 = createRS(ss21);
        ClusteredVariantEntity rs31 = createRS(ss31);

        mongoTemplate.insert(Arrays.asList(ss11, ss12, ss21, ss22, ss31, ss32), SubmittedVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(rs11, rs21, rs31), ClusteredVariantEntity.class);
    }

    public void assertDuplicateRSAccFileIsEmpty() throws IOException {
        assertTrue(Files.size(Paths.get(duplicateRsAccFile)) == 0);
    }

    public void assertDuplicateRSAccFileContains(Set<Long> expectedDuplicateAcc) throws IOException {
        Set<Long> rsAccInDuplicateFile = Files.lines(Paths.get(duplicateRsAccFile))
                .map(l -> Long.parseLong(l))
                .collect(Collectors.toSet());

        assertEquals(expectedDuplicateAcc, rsAccInDuplicateFile);
    }

    public static SubmittedVariantEntity createSS(String asm, int tax, String hash, String study, String contig,
                                                  Long ssAccession, Long rsAccession, Long start,
                                                  String reference, String alternate) {
        return new SubmittedVariantEntity(ssAccession, hash, asm, tax, study, contig, start, reference, alternate,
                rsAccession, false, false, false, false, 1);
    }

    public static ClusteredVariantEntity createRS(SubmittedVariantEntity sve) {
        Function<IClusteredVariant, String> hashingFunction = new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariant cv = new ClusteredVariant(sve.getReferenceSequenceAccession(), sve.getTaxonomyAccession(),
                sve.getContig(),
                sve.getStart(),
                new Variant(sve.getContig(), sve.getStart(), sve.getStart(),
                        sve.getReferenceAllele(),
                        sve.getAlternateAllele()).getType(),
                true, null);
        String hash = hashingFunction.apply(cv);
        return new ClusteredVariantEntity(sve.getClusteredVariantAccession(), hash, cv);
    }
}