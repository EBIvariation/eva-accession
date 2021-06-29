/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp2.runner;

import com.fasterxml.jackson.databind.JsonNode;
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
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.runner.CommandLineRunnerUtils;
import uk.ac.ebi.eva.accession.dbsnp2.batch.io.BzipLazyResource;
import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp2.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import javax.sql.DataSource;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_JOB;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.dbsnp2.runner.DbsnpJsonImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS;
import static uk.ac.ebi.eva.accession.dbsnp2.runner.DbsnpJsonImportVariantsJobLauncherCommandLineRunner.EXIT_WITH_ERRORS;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:application.properties")
@SpringBatchTest
public class DbsnpJsonImportVariantsJobLauncherCommandLineRunnerTest {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DataSource datasource;

    @Autowired
    private DbsnpJsonImportVariantsJobLauncherCommandLineRunner runner;

    @Autowired
    private FlatFileItemReader<JsonNode> variantReader;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private static File tempJsonInputFileToTestFailingJobs;

    private static String originalJsonInputFilePath;

    private static String originalJsonContent;

    private boolean originalInputParametersCaptured = false;

    @BeforeClass
    public static void initializeTempFile() throws Exception {
        tempJsonInputFileToTestFailingJobs = File.createTempFile("resumeFailingJob", ".json.bz2");
    }

    @AfterClass
    public static void deleteTempFile() throws Exception {
        tempJsonInputFileToTestFailingJobs.delete();
    }

    @Before
    public void setUp() throws Exception {
        if (!originalInputParametersCaptured) {
            originalJsonInputFilePath = inputParameters.getInput();
            originalJsonContent = getOriginalJsonContent(originalJsonInputFilePath);
            writeToTempJsonFile(originalJsonContent);
            originalInputParametersCaptured = true;
        }
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(IMPORT_DBSNP_JSON_VARIANTS_JOB);
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
        useOriginalJsonFile();
    }

    @After
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
    }

    @Test
    @DirtiesContext
    public void runSuccessfulJob() throws Exception {
        inputParameters.setForceImport(true);
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void restartCompletedJobThatIsAlreadyInTheRepository() throws Exception {
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());

        inputParameters.setForceRestart(true);
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void restartFailedJobThatIsAlreadyInTheRepository() throws Exception {
        useTempJsonFile();
        injectErrorIntoTempJson();
        JobInstance failingJobInstance = runJobAandCheckResults();

        inputParameters.setForceRestart(true);
        remediateTempVcfError();
        runJobBAndCheckRestart(failingJobInstance);
    }

    private JobInstance runJobAandCheckResults() throws Exception {
        runner.run();
        assertEquals(EXIT_WITH_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(IMPORT_DBSNP_JSON_VARIANTS_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                                                                         IMPORT_DBSNP_JSON_VARIANTS_STEP);
        //Ensure that only the first batch was written (batch size is 2 and error was at line#4)
        assertEquals(inputParameters.getChunkSize(), stepExecution.getWriteCount());

        return currentJobInstance;
    }

    private void runJobBAndCheckRestart(JobInstance failingJobInstance) throws Exception {
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(IMPORT_DBSNP_JSON_VARIANTS_JOB,
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

        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void resumeFailingJobFromCorrectChunk() throws Exception {
        // Jobs A, B, C are run chronological order; A and C have SAME parameters;
        // A is the job that is run after VCF fault injection (as part of the runTestWithFaultInjection method),
        // therefore should fail.
        // B is a job run with the original VCF without any faults (run separately), therefore should succeed.
        // C is a job with the same parameters as A run after VCF fault remediation (as part of the
        // runTestWithFaultInjection method), therefore should resume A and succeed.

        useTempJsonFile();
        injectErrorIntoTempJson();
        JobInstance failingJobInstance = runJobAandCheckResults();

        runJobBAndCheckResults();

        remediateTempVcfError();
        runJobCAndCheckResumption(failingJobInstance);
    }

    private void runJobBAndCheckResults() throws Exception {
        useOriginalJsonFile();
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());

        //Restore state so that Job C can continue running after fault remediation
        useTempJsonFile();
    }

    private void runJobCAndCheckResumption(JobInstance failingJobInstance) throws Exception {
        runner.run();
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(IMPORT_DBSNP_JSON_VARIANTS_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                                                                         IMPORT_DBSNP_JSON_VARIANTS_STEP);
        // Did we resume the previous failed job instance?
        assertEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());

        int numberOfLinesInJson = getNumberOfLinesInJsonString(originalJsonContent);
        // Test resumption point - did we pick up where we left off?
        // Ensure all the batches other than the first batch were processed
        assertEquals(numberOfLinesInJson - inputParameters.getChunkSize(), stepExecution.getWriteCount());
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    private void injectErrorIntoTempJson() throws Exception {
        // Intentionally inject error in entry#9 in the original Json
        String modifiedJsonContent = originalJsonContent.replace("3069077", "3069077jibberish");
        writeToTempJsonFile(modifiedJsonContent);
    }

    private void remediateTempVcfError() throws Exception {
        writeToTempJsonFile(originalJsonContent);
    }

    private void useOriginalJsonFile() throws Exception {
        inputParameters.setInput(originalJsonInputFilePath);
        variantReader.setResource(FileUtils.getResource(new File(originalJsonInputFilePath)));
    }

    private void useTempJsonFile() throws Exception {
        // The following does not actually change the wiring of the variantReader since the wiring happens before the tests
        // This setVcf is only to facilitate identifying jobs in the job repo by parameter
        // (those that use original vs temp JSON)
        inputParameters.setInput(tempJsonInputFileToTestFailingJobs.getAbsolutePath());
        /*
         * Change the auto-wired JSON for variantReader at runtime
         * Rationale:
         *  1) Why not use two test configurations, one for a JSON that fails validation and another for a JSON
         *  that won't and test resumption?
         *     Beginning Spring Boot 2, job resumption can only happen when input parameters to the restarted job
         *     is the same as the failed job.
         *     Therefore, a test to check resumption cannot have two different config files with different
         *     parameters.input.
         *     This test therefore creates a dynamic JSON and injects errors at runtime to the JSON thus preserving
         *     the input parameter but changing the JSON content.
         *  2) Why not artificially inject a variantReader exception?
         *     This will preclude us from verifying job resumption from a precise line in the JSON.
         */
        variantReader.setResource(FileUtils.getResource(tempJsonInputFileToTestFailingJobs));
    }

    private void writeToTempJsonFile(String modifiedJsonContent) throws IOException {
        OutputStream bzipOutputStream =
                new BzipLazyResource(tempJsonInputFileToTestFailingJobs.getAbsolutePath()).getOutputStream();
        bzipOutputStream.write(modifiedJsonContent.getBytes(StandardCharsets.UTF_8));
        bzipOutputStream.close();
    }

    private String getOriginalJsonContent(String inputJsonPath) throws IOException {
        InputStream bzipInputStream = new BzipLazyResource(inputJsonPath).getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(bzipInputStream));
        StringBuilder originalJsonContent = new StringBuilder();
        String read;
        while ((read = reader.readLine()) != null) {
            originalJsonContent.append(read).append(System.lineSeparator());
        }
        return originalJsonContent.toString();
    }

    private int getNumberOfLinesInJsonString(String jsonString) {
        return (int) Arrays.stream(jsonString.split(System.lineSeparator()))
                           .filter(line -> !line.startsWith("#"))
                           .count();
    }
}