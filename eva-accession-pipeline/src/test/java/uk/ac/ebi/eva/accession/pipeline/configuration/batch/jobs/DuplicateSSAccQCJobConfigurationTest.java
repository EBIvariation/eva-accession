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
package uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import gherkin.deps.com.google.gson.Gson;
import gherkin.deps.com.google.gson.GsonBuilder;
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
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.FixSpringMongoDbRule;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.DUPLICATE_SS_ACC_QC_STEP;
import static uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration.JOB_LAUNCHER_DUPLICATE_SS_ACC_QC_JOB;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:duplicate-ss-acc-qc-test.properties")
public class DuplicateSSAccQCJobConfigurationTest {

    private static final String TEST_DB = "test-db";
    private static final String duplicateSsAccFile = "src/test/resources/duplicateSSAcc.csv";

    @Autowired
    @Qualifier(JOB_LAUNCHER_DUPLICATE_SS_ACC_QC_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    private final Gson gson = new GsonBuilder().create();

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() throws IOException {
        this.mongoClient.dropDatabase(TEST_DB);
        Files.deleteIfExists(Paths.get(duplicateSsAccFile));
    }

    @After
    public void tearDown() throws IOException {
        this.mongoClient.dropDatabase(TEST_DB);
        Files.deleteIfExists(Paths.get(duplicateSsAccFile));
    }

    @Test
    public void contextLoads() {

    }

    @Test
    public void duplicateSSAccQCTest_NoDuplicate() throws IOException {
        SubmittedVariantEntity ss1 = createSS("GCA_000000001.1", 60711, "study1", "hash" + 1, "chr1", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity ss2 = createSS("GCA_000000001.1", 60711, "study2", "hash" + 2, "chr1", 2L, 2L, 100L, "C", "T");
        mongoTemplate.save(ss1, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        mongoTemplate.save(ss2, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_SS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertDuplicateSSAccFileIsEmpty();
    }

    @Test
    public void duplicateSSAccQCTest_NoDuplicateAsOneIsRemapped() throws IOException {
        SubmittedVariantEntity ss1 = createSS("GCA_000000001.1", 60711, "study1", "hash" + 1, "chr1", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity ss2 = createSSRemappedFrom("GCA_000000001.1", 60711, "study2", "hash" + 2, "chr1", 1L, 1L, 100L, "C", "T", "GCA_000000001.1");
        mongoTemplate.save(ss1, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        mongoTemplate.save(ss2, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_SS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertDuplicateSSAccFileIsEmpty();
    }

    @Test
    public void duplicateSSAccQCTest_DuplicateInSVE() throws IOException {
        SubmittedVariantEntity ss1 = createSS("GCA_000000001.1", 60711, "study1", "hash" + 1, "chr1", 1L, 1L, 100L, "C", "T");
        SubmittedVariantEntity ss2 = createSS("GCA_000000001.1", 60711, "study2", "hash" + 2, "chr1", 1L, 1L, 100L, "C", "T");
        mongoTemplate.save(ss1, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        mongoTemplate.save(ss2, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_SS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        Set<Long> expectedSSAccs = new HashSet<>();
        expectedSSAccs.add(1L);

        assertDuplicateSSAccFileContains(expectedSSAccs);
    }

    @Test
    public void duplicateSSAccQCTest_DuplicateInSVEAndDBSNP() throws IOException {
        SubmittedVariantEntity ss1 = createSS("GCA_000000001.1", 60711, "study1", "hash" + 1, "chr1", 1L, 1L, 100L, "C", "T");
        DbsnpSubmittedVariantEntity dbsnp1 = createDbsnpSS("GCA_000000001.1", 60711, "study2", "hash" + 2, "chr1", 1L, 1L, 100L, "C", "T");
        mongoTemplate.save(ss1, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        mongoTemplate.save(dbsnp1, mongoTemplate.getCollectionName(DbsnpSubmittedVariantEntity.class));

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DUPLICATE_SS_ACC_QC_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        Set<Long> expectedSSAccs = new HashSet<>();
        expectedSSAccs.add(1L);

        assertDuplicateSSAccFileContains(expectedSSAccs);
    }


    public void assertDuplicateSSAccFileIsEmpty() throws IOException {
        assertTrue(Files.size(Paths.get(duplicateSsAccFile)) == 0);
    }

    public void assertDuplicateSSAccFileContains(Set<Long> expectedDuplicateAcc) throws IOException {
        Set<Long> ssAccInDuplicateFile = Files.lines(Paths.get(duplicateSsAccFile))
                .map(l -> Long.parseLong(l.split(" ")[0]))
                .collect(Collectors.toSet());

        assertEquals(expectedDuplicateAcc, ssAccInDuplicateFile);
    }

    public static SubmittedVariantEntity createSS(String asm, int tax, String hash, String study, String contig,
                                                  Long ssAccession, Long rsAccession, Long start,
                                                  String reference, String alternate) {
        return new SubmittedVariantEntity(ssAccession, hash, asm, tax, study, contig, start, reference, alternate,
                rsAccession, false, false, false, false, 1);
    }

    public static SubmittedVariantEntity createSSRemappedFrom(String asm, int tax, String hash, String study, String contig,
                                                              Long ssAccession, Long rsAccession, Long start,
                                                              String reference, String alternate, String remappedFrom) {
        SubmittedVariantEntity submittedVariantEntity = createSS(asm, tax, hash, study, contig,
                ssAccession, rsAccession, start, reference, alternate);
        submittedVariantEntity.setRemappedFrom(remappedFrom);

        return submittedVariantEntity;
    }

    public static DbsnpSubmittedVariantEntity createDbsnpSS(String asm, int tax, String hash, String study, String contig,
                                                            Long ssAccession, Long rsAccession, Long start,
                                                            String reference, String alternate) {
        return new DbsnpSubmittedVariantEntity(ssAccession, hash, asm, tax, study, contig, start, reference, alternate,
                rsAccession, false, false, false, false, 1);
    }

}