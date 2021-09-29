/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.accession.clustering.runner;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.clustering.parameters.CountParameters;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.runner.CommandLineRunnerUtils;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_MONGO_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_VCF_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_VCF_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes={BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class ClusteringCommandLineRunnerTest {

    private static final String TEST_DB = "test-db";

    private static final int TAXONOMY = 60711;

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final String PROJECT = "PRJ1";

    @Autowired
    private Long accessioningMonotonicInitSs;

    @Autowired
    private Long accessioningMonotonicInitRs;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DataSource datasource;

    @Autowired
    private ClusteringCommandLineRunner runner;

    @Autowired
    private VcfReader vcfReader;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());


    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private static String originalVcfInputFilePath;

    private static String originalVcfContent;

    private static File tempVcfInputFileToTestFailingJobs;

    private boolean originalInputParametersCaptured = false;

    private SubmittedVariantEntity evaSS1, dbsnpSS2, evaSS3, dbsnpSS4, evaSS5, dbsnpSS6, dbsnpSS7, evaSS8, evaSS9;

    private ClusteredVariantEntity dbsnpRS1, evaRS2, dbsnpRS3, evaRS4, dbsnpRS5;

    @Autowired
    private RestTemplate restTemplate;

    private MockRestServiceServer mockServer;

    private static class RSLocus {
        String assembly;
        String contig;
        long start;
        VariantType type;

        public RSLocus(String assembly, String contig, long start, VariantType type) {
            this.assembly = assembly;
            this.contig = contig;
            this.start = start;
            this.type = type;
        }
    }

    private RSLocus rsLocus1, rsLocus2, rsLocus3, rsLocus4, rsLocus5;

    @Autowired
    private CountParameters countParameters;
    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @BeforeClass
    public static void initializeTempFile() throws Exception {
        tempVcfInputFileToTestFailingJobs = File.createTempFile("resumeFailingJob", ".vcf.gz");
    }

    @AfterClass
    public static void deleteTempFile() throws Exception {
        tempVcfInputFileToTestFailingJobs.delete();
    }

    @Before
    public void setUp() throws Exception {
        if (!originalInputParametersCaptured) {
            originalVcfInputFilePath = inputParameters.getVcf();
            originalVcfContent = getOriginalVcfContent(originalVcfInputFilePath);
            writeToTempVCFFile(originalVcfContent);
            originalInputParametersCaptured = true;
        }
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(CLUSTERING_FROM_VCF_JOB);
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
        useOriginalVcfFile();

        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
        mongoTemplate.getDb().drop();
    }

    @After
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
        mongoTemplate.getDb().drop();
    }


    @Test
    @UsingDataSet(locations = {"/test-data/submittedVariantEntityMongoReader.json"})
    public void runMongoJobWithNoErrors() throws JobExecutionException {
        runner.setJobNames(CLUSTERING_FROM_MONGO_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    public void runClusteringMongoJobOnRemappedVariantsWithNoErrors() throws JobExecutionException {
        /*
         * SS           RS                  RS_LOCUS    Remapped
         * evaSS1       dbsnpRS1            rsLocus1    Y
         * dbsnpSS2     evaRS2              rsLocus1    Y
         * evaSS3       evaRS2              rsLocus2    N
         * dbsnpSS4     dbsnpRS3            rsLocus3    N
         * evaSS5       evaRS4              rsLocus3    Y
         * dbsnpSS6     evaRS4              rsLocus4    N
         * dbsnpSS7     evaRS4              rsLocus4    Y
         * evaSS8       Unassigned          rsLocus4    N
         * evaSS9       dbsnpRS5            rsLocus5    Y
         */
        setupRSAndSS();
        runner.setJobNames(CLUSTERING_FROM_MONGO_JOB);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        /*
        See results here: https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=972582890
        */

    }

    private void setupRSAndSS() {
        rsLocus1 = new RSLocus(ASSEMBLY, "chr1", 100L,  VariantType.SNV);
        rsLocus2 = new RSLocus(ASSEMBLY, "chr1", 101L,  VariantType.SNV);
        rsLocus3 = new RSLocus(ASSEMBLY, "chr1", 102L,  VariantType.SNV);
        rsLocus4 = new RSLocus(ASSEMBLY, "chr1", 103L,  VariantType.SNV);
        rsLocus5 = new RSLocus(ASSEMBLY, "chr1", 104L,  VariantType.SNV);
        dbsnpRS1 = createRS(1L, rsLocus1, true);
        evaRS2 = createRS(5L, rsLocus2, false);
        dbsnpRS3 = createRS(2L, rsLocus3, false);
        evaRS4 = createRS(6L, rsLocus3, true);
        dbsnpRS5 = createRS(3L, rsLocus5, true);

        evaSS1 = createRemappedSS(5L, dbsnpRS1.getAccession(), rsLocus1, "A", "T", true);
        dbsnpSS2 = createRemappedSS(1L, evaRS2.getAccession(), rsLocus1, "A", "G", true);
        evaSS3 = createRemappedSS(6L, evaRS2.getAccession(), rsLocus2, "C", "G", false);
        dbsnpSS4 = createRemappedSS(2L, dbsnpRS3.getAccession(), rsLocus3, "G", "A", false);
        evaSS5 = createRemappedSS(7L, evaRS4.getAccession(), rsLocus3, "G", "A", true);
        dbsnpSS6 = createRemappedSS(3L, evaRS4.getAccession(), rsLocus4, "T", "C", false);
        dbsnpSS7 = createRemappedSS(4L, evaRS4.getAccession(), rsLocus4, "T", "A", true);
        evaSS8 = createRemappedSS(8L, null, rsLocus4, "T", "G", false);
        evaSS9 = createRemappedSS(9L, dbsnpRS5.getAccession(), rsLocus5, "A", "T", true);
    }

    private SubmittedVariantEntity createRemappedSS(Long ssAccession, Long rsAccession, RSLocus rsLocus, String reference,
                                                    String alternate, boolean remapped) {
        Function<ISubmittedVariant, String> hashingFunction =  new SubmittedVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        SubmittedVariant submittedVariant = new SubmittedVariant(ASSEMBLY, TAXONOMY, PROJECT, rsLocus.contig,
                                                                 rsLocus.start, reference, alternate, rsAccession);
        String hash = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(ssAccession, hash, submittedVariant,
                                                                                   1);
        if (remapped) {
            submittedVariantEntity.setRemappedFrom("ASM1");
        }
        if (ssAccession >= accessioningMonotonicInitSs) {
            mongoTemplate.save(submittedVariantEntity, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        } else {
            mongoTemplate.save(submittedVariantEntity,
                               mongoTemplate.getCollectionName(DbsnpSubmittedVariantEntity.class));
        }
        return submittedVariantEntity;
    }

    private ClusteredVariantEntity createRS(Long rsAccession, RSLocus rsLocus, boolean remapped) {
        Function<IClusteredVariant, String> hashingFunction =  new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariant clusteredVariant = new ClusteredVariant(rsLocus.assembly, TAXONOMY, rsLocus.contig,
                                                                 rsLocus.start, rsLocus.type, false, null);
        String hash = hashingFunction.apply(clusteredVariant);
        ClusteredVariantEntity clusteredVariantEntity = new ClusteredVariantEntity(rsAccession, hash, clusteredVariant);
        // Note that only SS ID entries are created post remapping.
        // If we should simulate that environment,
        // we should not create RS ID entries in the database for remapped variants
        // like we did for SS IDs. The RS IDs for the remapped assembly will be created after the clustering process.
        if (!remapped) {
            if (rsAccession >= accessioningMonotonicInitRs) {
                mongoTemplate.save(clusteredVariantEntity, mongoTemplate.getCollectionName(ClusteredVariantEntity.class));
            } else {
                mongoTemplate.save(clusteredVariantEntity,
                                   mongoTemplate.getCollectionName(DbsnpClusteredVariantEntity.class));
            }
        }
        return clusteredVariantEntity;
    }

    @Test
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void runJobWithNoErrors() throws JobExecutionException {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    public void runJobWithNoName() throws JobExecutionException {
        runner.setJobNames(null);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    public void runNonExistentJob() throws JobExecutionException {
        runner.setJobNames("NOT_EXISTENT_JOB");
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void restartCompletedJobThatIsAlreadyInTheRepository() throws Exception {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        inputParameters.setForceRestart(true);
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void restartFailedJobThatIsAlreadyInTheRepository() throws Exception {
        useTempVcfFile();
        injectErrorIntoTempVcf();
        JobInstance failingJobInstance = runJobAandCheckResults();

        inputParameters.setForceRestart(true);
        remediateTempVcfError();
        runJobBAndCheckRestart(failingJobInstance);
    }

    private JobInstance runJobAandCheckResults() throws Exception {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_VCF_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                                                                         CLUSTERING_FROM_VCF_STEP);
        //Ensure that only the first batch was written (batch size is 2 and error was at line#4)
        assertEquals(inputParameters.getChunkSize(), stepExecution.getWriteCount());

        return currentJobInstance;
    }

    private void runJobBAndCheckRestart(JobInstance failingJobInstance) throws Exception {
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_VCF_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        assertNotEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());
    }

    @Test
    @DirtiesContext
    public void forceRestartButNoJobInTheRepository() throws Exception {
        inputParameters.setForceRestart(true);
        assertEquals(Collections.EMPTY_LIST, jobExplorer.getJobNames());
        runner.run();

        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void resumeFailingJobFromCorrectChunk() throws Exception {
        // Jobs A, B, C are run chronological order; A and C have SAME parameters;
        // A is the job that is run after VCF fault injection (as part of the runTestWithFaultInjection method),
        // therefore should fail.
        // B is a job run with the original VCF without any faults (run separately), therefore should succeed.
        // C is a job with the same parameters as A run after VCF fault remediation (as part of the
        // runTestWithFaultInjection method), therefore should resume A and succeed.

        useTempVcfFile();
        injectErrorIntoTempVcf();
        JobInstance failingJobInstance = runJobAandCheckResults();

        runJobBAndCheckResults();

        remediateTempVcfError();
        runJobCAndCheckResumption(failingJobInstance);
    }

    private void runJobBAndCheckResults() throws Exception {
        useOriginalVcfFile();
        runner.run();
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        //Restore state so that Job C can continue running after fault remediation
        useTempVcfFile();
    }

    private void runJobCAndCheckResumption(JobInstance failingJobInstance) throws Exception {
        runner.run();
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CLUSTERING_FROM_VCF_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                                                                         CLUSTERING_FROM_VCF_STEP);
        // Did we resume the previous failed job instance?
        assertEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());

        int numberOfLinesInVcf = getNumberOfLinesInVcfString(originalVcfContent);
        // Test resumption point - did we pick up where we left off?
        // Ensure all the batches other than the first batch were processed
        assertEquals(numberOfLinesInVcf - inputParameters.getChunkSize(), stepExecution.getWriteCount());
        assertEquals(ClusteringCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    private void injectErrorIntoTempVcf() throws Exception {
        String modifiedVcfContent = originalVcfContent.replace("ss5000000004", "4ss--jibberish");
        // Inject error in the VCF file to cause processing to stop at variant#4
        writeToTempVCFFile(modifiedVcfContent);
    }

    private void remediateTempVcfError() throws Exception {
        writeToTempVCFFile(originalVcfContent);
    }

    private void useOriginalVcfFile() throws Exception {
        inputParameters.setVcf(originalVcfInputFilePath);
        vcfReader.setResource(FileUtils.getResource(new File(originalVcfInputFilePath)));
    }

    private void useTempVcfFile() throws Exception {
        // The following does not actually change the wiring of the vcfReader since the wiring happens before the tests
        // This setVcf is only to facilitate identifying jobs in the job repo by parameter
        // (those that use original vs temp VCF)
        inputParameters.setVcf(tempVcfInputFileToTestFailingJobs.getAbsolutePath());
        /*
             * Change the auto-wired VCF for VCFReader at runtime
             * Rationale:
             *  1) Why not use two test configurations, one for a VCF that fails validation and another for a VCF
             *  that won't and test resumption?
             *     Beginning Spring Boot 2, job resumption can only happen when input parameters to the restarted job
             *     is the same as the failed job.
             *     Therefore, a test to check resumption cannot have two different config files with different
             *     parameters.vcf.
             *     This test therefore creates a dynamic VCF and injects errors at runtime to the VCF thus preserving
             *     the VCF parameter but changing the VCF content.
             *  2) Why not artificially inject a VcfReader exception?
             *     This will preclude us from verifying job resumption from a precise line in the VCF.
         */
        vcfReader.setResource(FileUtils.getResource(tempVcfInputFileToTestFailingJobs));
    }

    private void writeToTempVCFFile(String modifiedVCFContent) throws IOException {
        FileOutputStream outputStream = new FileOutputStream(tempVcfInputFileToTestFailingJobs.getAbsolutePath());
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(outputStream);
        gzipOutputStream.write(modifiedVCFContent.getBytes(StandardCharsets.UTF_8));
        gzipOutputStream.close();
    }

    private String getOriginalVcfContent(String inputVcfPath) throws Exception {
        StringBuilder originalVCFContent = new StringBuilder();

        GZIPInputStream gzipInputStream = new GZIPInputStream(new FileInputStream(inputVcfPath));
        BufferedReader reader = new BufferedReader(new InputStreamReader(gzipInputStream));

        String read;
        while ((read = reader.readLine()) != null) {
            originalVCFContent.append(read).append(System.lineSeparator());
        }
        return originalVCFContent.toString();
    }

    private int getNumberOfLinesInVcfString(String vcfString) {
        return (int) Arrays.stream(vcfString.split(System.lineSeparator()))
                           .filter(line -> !line.startsWith("#"))
                           .count();
    }
}