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
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import javax.sql.DataSource;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.doNothing;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_JOB;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.runner.runnerUtil.deleteTemporaryContigAndVariantFiles;
import static uk.ac.ebi.eva.accession.pipeline.runner.runnerUtil.getOriginalVcfContent;
import static uk.ac.ebi.eva.accession.pipeline.runner.runnerUtil.injectErrorIntoTempVcf;
import static uk.ac.ebi.eva.accession.pipeline.runner.runnerUtil.remediateTempVcfError;
import static uk.ac.ebi.eva.accession.pipeline.runner.runnerUtil.useOriginalVcfFile;
import static uk.ac.ebi.eva.accession.pipeline.runner.runnerUtil.useTempVcfFile;
import static uk.ac.ebi.eva.accession.pipeline.runner.runnerUtil.writeToTempVCFFile;

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
            writeToTempVCFFile(originalVcfContent, tempVcfInputFileToTestFailingJobs);
            originalInputParametersCaptured = true;
        }
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(CREATE_SUBSNP_ACCESSION_JOB);
        deleteTemporaryContigAndVariantFiles(inputParameters, tempVcfOutputDir);
        useOriginalVcfFile(inputParameters, originalVcfInputFilePath, vcfReader);

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

    /*
    * Separated this test from the rest of the tests in EvaAccessionJobLauncherCommandLineRunnerTest,
    * as we have to Mock(Spy to be exact) on the SubmittedVariantAccessioningService bean in order to reuse the same
    * without shutting down its accession generator.
    *
    * Ideally, we should not be Spying but rather the jobs when restarting should be using a new instance of the service.
    * but it was tricky to inject, hence the workaround.
    * */
    @Test
    @DirtiesContext
    public void restartFailedJobThatIsAlreadyInTheRepository() throws Exception {
        useTempVcfFile(inputParameters, tempVcfInputFileToTestFailingJobs, vcfReader);
        String modifiedVcfContent = originalVcfContent.replace("76852", "76852jibberish");
        injectErrorIntoTempVcf(modifiedVcfContent, tempVcfInputFileToTestFailingJobs);
        JobInstance failingJobInstance = runJobAandCheckResults();

        mongoTemplate.dropCollection(SubmittedVariantEntity.class);

        inputParameters.setForceRestart(true);
        remediateTempVcfError(originalVcfContent, tempVcfInputFileToTestFailingJobs);
        deleteTemporaryContigAndVariantFiles(inputParameters, tempVcfOutputDir); //left behind by unsuccessful job A
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

}