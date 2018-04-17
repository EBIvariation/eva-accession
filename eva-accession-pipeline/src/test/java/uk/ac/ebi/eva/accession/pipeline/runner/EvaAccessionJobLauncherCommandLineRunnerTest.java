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
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import javax.sql.DataSource;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerTestConfiguration.TEST_JOB_NAME;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerTestConfiguration.TEST_STEP_1_NAME;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes={RunnerTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
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

    JobRepositoryTestUtils jobRepositoryTestUtils;


    @Before
    public void setUp() throws Exception {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(TEST_JOB_NAME);
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
    public void forceRestartForJobThatIsAlreadyInTheRepository() throws Exception {
        long jobId = createStartedJobExecution(TEST_JOB_NAME, inputParameters.toJobParameters());
        long stepId = addStartedStepToJobExecution(jobId, TEST_STEP_1_NAME);

        inputParameters.setForceRestart(true);
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        assertEquals(BatchStatus.FAILED, jobExplorer.getStepExecution(jobId, stepId).getStatus());
        assertEquals(BatchStatus.FAILED, jobExplorer.getJobExecution(jobId).getStatus());
        JobExecution lastJobExecution = jobRepository.getLastJobExecution(TEST_JOB_NAME,
                                                                          inputParameters.toJobParameters());
        assertEquals(BatchStatus.COMPLETED, lastJobExecution.getStatus());
        assertTrue(lastJobExecution.getStepExecutions().stream()
                                   .allMatch(s -> s.getStatus().equals(BatchStatus.COMPLETED)));
    }

    @Test
    public void runJobThatIsAlreadyInTheRepositoryWithoutForcingRestart() throws Exception {
        long jobId = createStartedJobExecution(TEST_JOB_NAME, inputParameters.toJobParameters());
        long stepId = addStartedStepToJobExecution(jobId, TEST_STEP_1_NAME);

        inputParameters.setForceRestart(false);
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
        assertEquals(BatchStatus.STARTED, jobExplorer.getStepExecution(jobId, stepId).getStatus());
        assertEquals(BatchStatus.STARTED, jobExplorer.getJobExecution(jobId).getStatus());
    }

    @Test
    public void forceRestartButNoJobInTheRepository() throws Exception {
        inputParameters.setForceRestart(true);
        assertEquals(Collections.EMPTY_LIST, jobExplorer.getJobNames());
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    private long createStartedJobExecution(String jobName, JobParameters jobParameters) throws Exception {
        JobExecution jobExecution = jobRepository.createJobExecution(jobName, jobParameters);
        jobExecution.setStatus(BatchStatus.STARTED);
        jobRepository.update(jobExecution);
        return jobExecution.getJobId();
    }

    private long addStartedStepToJobExecution(long jobExecutionId, String stepName) {
        JobExecution jobExecution = jobExplorer.getJobExecution(jobExecutionId);
        StepExecution stepExecution = jobExecution.createStepExecution(stepName);
        jobRepository.add(stepExecution);
        long stepId = stepExecution.getId();
        stepExecution.setStatus(BatchStatus.STARTED);
        jobRepository.update(stepExecution);
        return stepId;
    }
}