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
import org.springframework.batch.core.launch.JobOperator;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import javax.sql.DataSource;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerTestConfiguration.TEST_JOB_NAME;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes={RunnerTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class EvaAccessionJobLauncherCommandLineRunnerTest {

//    @Autowired
//    private JobLauncher jobLauncher;
//
//    @Autowired
//    private JobExplorer jobExplorer;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private DataSource datasource;

    @Autowired
    EvaAccessionJobLauncherCommandLineRunner runner;

//    @Autowired
//    JobRepositoryTestUtils jobRepositoryTestUtils;

    JobRepositoryTestUtils jobRepositoryTestUtils;


    @Before
    public void setUp() throws Exception {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(TEST_JOB_NAME);
    }

    @After
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    public void runJobWithNoErrors() throws Exception {
        runner.run("");

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    public void runJobWithNoName() throws Exception {
        runner.setJobNames(null);
        runner.run("");

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    public void forceRestarForJobThatIsAlreadyInTheRepository() throws Exception {
        jobRepository.createJobExecution(TEST_JOB_NAME, inputParameters.toJobParameters());

        inputParameters.setForceRestart(true);

        runner.run("");

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    public void forceRestartButNoJobInTheRepository() throws Exception {
        inputParameters.setForceRestart(true);

        runner.run("");

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }


    @Test
    public void runJobThatIsAlreadyInTheRepositoryWithoutForcingRestart() throws Exception {
        jobRepository.createJobExecution(TEST_JOB_NAME, inputParameters.toJobParameters());

        inputParameters.setForceRestart(false);

        runner.run("");

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }
}