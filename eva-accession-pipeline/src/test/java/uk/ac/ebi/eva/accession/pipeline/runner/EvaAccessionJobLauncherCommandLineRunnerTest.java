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
import org.junit.Ignore;
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
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import javax.sql.DataSource;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
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
public class EvaAccessionJobLauncherCommandLineRunnerTest {

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
    }

    @After
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
    }

    @Test
    public void runJobWithNoErrors() throws Exception {
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    public void runJobWithNoName() throws Exception {
        runner.setJobNames(null);
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @Ignore
    public void restartCompletedJobThatIsAlreadyInTheRepository() throws Exception {
        runner.run();
        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        deleteTemporaryContigAndVariantFiles(inputParameters, tempVcfOutputDir);

        inputParameters.setForceRestart(true);
        runner.run();
        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
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

    @Test
    @DirtiesContext
    public void forceRestartButNoJobInTheRepository() throws Exception {
        inputParameters.setForceRestart(true);
        assertEquals(Collections.EMPTY_LIST, jobExplorer.getJobNames());
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    @Ignore
    public void resumeFailingJobFromCorrectChunk() throws Exception {
        // Jobs A, B, C are run chronological order; A and C have SAME parameters;
        // A is the job that is run after VCF fault injection (as part of the runTestWithFaultInjection method),
        // therefore should fail.
        // B is a job run with the original VCF without any faults (run separately), therefore should succeed.
        // C is a job with the same parameters as A run after VCF fault remediation (as part of the
        // runTestWithFaultInjection method), therefore should resume A and succeed.

        useTempVcfFile(inputParameters, tempVcfInputFileToTestFailingJobs, vcfReader);
        String modifiedVcfContent = originalVcfContent.replace("76852", "76852jibberish");
        injectErrorIntoTempVcf(modifiedVcfContent, tempVcfInputFileToTestFailingJobs);
        JobInstance failingJobInstance = runJobAandCheckResults();

        runJobBAndCheckResults();

        remediateTempVcfError(originalVcfContent, tempVcfInputFileToTestFailingJobs);
        runJobCAndCheckResumption(failingJobInstance);
    }

    private void runJobBAndCheckResults() throws Exception {
        useOriginalVcfFile(inputParameters, originalVcfInputFilePath, vcfReader);
        // Back up contig and variant files (left behind by previous unsuccessful job A) to temp folder
        // so as to not interfere with this job's execution which uses the original VCF file
        backUpContigAndVariantFilesToTempFolder();

        runner.run();
        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        //Restore state so that Job C can continue running after fault remediation
        useTempVcfFile(inputParameters, tempVcfInputFileToTestFailingJobs, vcfReader);
        restoreContigAndVariantFilesFromTempFolder();
    }

    private void runJobCAndCheckResumption(JobInstance failingJobInstance) throws Exception {
        runner.run();
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(CREATE_SUBSNP_ACCESSION_JOB,
                        jobExplorer,
                        inputParameters.toJobParameters())
                .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance,
                CREATE_SUBSNP_ACCESSION_STEP);
        // Did we resume the previous failed job instance?
        assertEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());

        int numberOfLinesInVcf = getNumberOfLinesInVcfString(originalVcfContent);
        int numberOfNonVariants = 1; //TBGI000010 is a non-variant
        // Test resumption point - did we pick up where we left off?
        // Ensure all the batches other than the first batch were processed
        assertEquals(numberOfLinesInVcf - inputParameters.getChunkSize() - numberOfNonVariants,
                stepExecution.getWriteCount());
        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }


    private void backUpContigAndVariantFilesToTempFolder() {
        moveFile(Paths.get(originalVcfOutputFilePath + ".contigs"),
                Paths.get(tempVcfOutputDir + "/accession-output.vcf.contigs"));
        moveFile(Paths.get(originalVcfOutputFilePath + ".variants"),
                Paths.get(tempVcfOutputDir + "/accession-output.vcf.variants"));
    }

    private void restoreContigAndVariantFilesFromTempFolder() {
        moveFile(Paths.get(tempVcfOutputDir + "/accession-output.vcf.contigs"),
                Paths.get(Paths.get(originalVcfOutputFilePath).getParent() + "/accession-output.vcf.contigs"));
        moveFile(Paths.get(tempVcfOutputDir + "/accession-output.vcf.variants"),
                Paths.get(Paths.get(originalVcfOutputFilePath).getParent() + "/accession-output.vcf.variants"));
    }

    private void moveFile(Path source, Path destination) {
        try {
            Files.move(source, destination, StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception ex) {
            if (!(ex instanceof NoSuchFileException)) {
                throw new RuntimeException(ex);
            }
        }
    }

    private int getNumberOfLinesInVcfString(String vcfString) {
        return (int) Arrays.stream(vcfString.split(System.lineSeparator()))
                .filter(line -> !line.startsWith("#"))
                .count();
    }
}