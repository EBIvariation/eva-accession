/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.accession.pipeline.runner;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.mock.mockito.SpyBean;
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
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.runner.CommandLineRunnerUtils;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_JOB;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
@SpringBatchTest
public class RestartFailedJobTest {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DataSource datasource;

    @Autowired
    private EvaAccessionJobLauncherCommandLineRunner runner;

    @Autowired
    private VcfReader vcfReader;

    @Autowired
    private CountServiceParameters countServiceParameters;

    @Autowired
    @Qualifier("COUNT_STATS_REST_TEMPLATE")
    private RestTemplate restTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    private MockRestServiceServer mockServer;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private static File tempVcfInputFileToTestFailingJobs;

    private static Path tempVcfOutputDir;

    private static String originalVcfInputFilePath;

    private static String originalVcfOutputFilePath;

    private static String originalVcfContent;

    private boolean originalInputParametersCaptured = false;

    @SpyBean
    private SubmittedVariantAccessioningService accessioningServiceSpy;

    @BeforeClass
    public static void initializeTempFile() throws Exception {
        tempVcfInputFileToTestFailingJobs = File.createTempFile("resumeFailingJob", ".vcf.gz");
        tempVcfOutputDir = Files.createTempDirectory("contigs_variants_dir");
    }

    @AfterClass
    public static void deleteTempFile() {
        tempVcfInputFileToTestFailingJobs.delete();
    }

    @Before
    public void setUp() throws Exception {
        if (!originalInputParametersCaptured) {
            originalVcfInputFilePath = inputParameters.getVcf();
            originalVcfOutputFilePath = inputParameters.getOutputVcf();
            originalVcfContent = getOriginalVcfContent(originalVcfInputFilePath);
            writeToTempVCFFile(originalVcfContent);
            originalInputParametersCaptured = true;
        }
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(CREATE_SUBSNP_ACCESSION_JOB);
        deleteTemporaryContigAndVariantFiles();
        useOriginalVcfFile();

        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));

        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
        // Mock the behavior of shutDownAccessionGenerator method to do nothing
        doNothing().when(accessioningServiceSpy).shutDownAccessionGenerator();

    }

    @After
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
    }

    @Test
    @DirtiesContext
    public void restartFailedJobThatIsAlreadyInTheRepository() throws Exception {
        useTempVcfFile();
        injectErrorIntoTempVcf();
        JobInstance failingJobInstance = runJobAandCheckResults();

        mongoTemplate.dropCollection(SubmittedVariantEntity.class);

        inputParameters.setForceRestart(true);
        remediateTempVcfError();
        deleteTemporaryContigAndVariantFiles(); //left behind by unsuccessful job A
        runJobBAndCheckRestart(failingJobInstance);
    }

    private JobInstance runJobAandCheckResults() throws Exception {
        runner.run();
        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CREATE_SUBSNP_ACCESSION_JOB,
                        jobExplorer,
                        inputParameters.toJobParameters())
                .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                CREATE_SUBSNP_ACCESSION_STEP);
        //Ensure that only the first batch was written (batch size is 5 and error was at line#9)
        assertEquals(inputParameters.getChunkSize(), stepExecution.getWriteCount());

        return currentJobInstance;
    }

    private void runJobBAndCheckRestart(JobInstance failingJobInstance) throws Exception {
        runner.run();
        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CREATE_SUBSNP_ACCESSION_JOB,
                        jobExplorer,
                        inputParameters.toJobParameters())
                .getJobInstance();
        assertNotEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());
    }

    private void injectErrorIntoTempVcf() throws Exception {
        String modifiedVcfContent = originalVcfContent.replace("76852", "76852jibberish");
        // Inject error in the VCF file to cause processing to stop at variant#9
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

    private void deleteTemporaryContigAndVariantFiles() throws Exception {
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf()));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + ".variants"));
        Files.deleteIfExists(Paths.get(inputParameters.getOutputVcf() + ".contigs"));
        Files.deleteIfExists(
                Paths.get(tempVcfOutputDir + "/accession-output.vcf.variants"));
        Files.deleteIfExists(
                Paths.get(tempVcfOutputDir + "/accession-output.vcf.contigs"));
    }

}