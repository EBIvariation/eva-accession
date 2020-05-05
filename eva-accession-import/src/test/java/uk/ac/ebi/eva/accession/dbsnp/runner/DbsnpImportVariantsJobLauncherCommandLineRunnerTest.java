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
package uk.ac.ebi.eva.accession.dbsnp.runner;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.runner.CommandLineRunnerUtils;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp.test.BatchTestConfiguration;

import javax.sql.DataSource;

import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_JOB;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:validate-contigs-fail.properties")
public class DbsnpImportVariantsJobLauncherCommandLineRunnerTest implements ApplicationContextAware {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DataSource datasource;

    @Autowired
    private DbsnpImportVariantsJobLauncherCommandLineRunner runner;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private ApplicationContext applicationContext;

    @Before
    public void setUp() throws Exception {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        jobRepositoryTestUtils.removeJobExecutions();
        runner.setJobNames(IMPORT_DBSNP_VARIANTS_JOB);
        inputParameters.setForceRestart(false);
    }

    @After
    public void tearDown() throws Exception {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    @DirtiesContext
    public void runSuccessfulJob() throws Exception {
        inputParameters.setForceImport(true);
        runner.run();
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void runFailingJob() throws Exception {
        inputParameters.setForceImport(false);
        runner.run();
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void restartCompletedJobThatIsAlreadyInTheRepository() throws Exception {
        inputParameters.setForceImport(true);
        runner.run();
        long instanceIdAfterFirstSuccessfulJob = jobExplorer.getJobInstances(IMPORT_DBSNP_VARIANTS_JOB, 0, 1).get(0)
                                                            .getInstanceId();
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());

        // We need to do this to prevent the cached reader bean (which was closed after the first run) from being reused
        resetBean("fastaSynonymSequenceReader");
        runner.run();
        long instanceIdAfterSecondSuccessfulJob = jobExplorer.getJobInstances(IMPORT_DBSNP_VARIANTS_JOB, 0, 1).get(0)
                                                             .getInstanceId();
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        assertNotEquals(instanceIdAfterFirstSuccessfulJob, instanceIdAfterSecondSuccessfulJob);
    }

    // See https://stackoverflow.com/a/54356616
    private void resetBean(String beanName) {
        GenericApplicationContext genericApplicationContext = (GenericApplicationContext) applicationContext;
        BeanDefinition bd = genericApplicationContext.getBeanDefinition(beanName);
        genericApplicationContext.removeBeanDefinition(beanName);
        genericApplicationContext.registerBeanDefinition(beanName, bd);
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext)
            throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Test
    @DirtiesContext
    public void resumeFailingJob() throws Exception {
        inputParameters.setForceImport(false);
        runner.run();
        long instanceIdAfterFailingJob = jobExplorer.getJobInstances(IMPORT_DBSNP_VARIANTS_JOB, 0, 1).get(0)
                                                    .getInstanceId();
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());

        inputParameters.setForceImport(true);
        runner.run();
        long instanceIdAfterSuccessfulJob = jobExplorer.getJobInstances(IMPORT_DBSNP_VARIANTS_JOB, 0, 1).get(0)
                                                       .getInstanceId();

        assertEquals(instanceIdAfterFailingJob, instanceIdAfterSuccessfulJob);
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void restartFailedJobThatIsAlreadyInTheRepository() throws Exception {
        JobInstance failingJobInstance = runJobAandCheckResults();
        inputParameters.setForceRestart(true);
        inputParameters.setForceImport(true);
        runJobBAndCheckRestart(failingJobInstance);
    }

    private JobInstance runJobAandCheckResults() throws Exception {
        runner.run();
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());

        return CommandLineRunnerUtils.getLastJobExecution(IMPORT_DBSNP_VARIANTS_JOB,
                                                          jobExplorer,
                                                          inputParameters.toJobParameters())
                                     .getJobInstance();
    }

    private void runJobBAndCheckRestart(JobInstance failingJobInstance) throws Exception {
        runner.run();
        assertEquals(DbsnpImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(IMPORT_DBSNP_VARIANTS_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        assertNotEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());
    }
}