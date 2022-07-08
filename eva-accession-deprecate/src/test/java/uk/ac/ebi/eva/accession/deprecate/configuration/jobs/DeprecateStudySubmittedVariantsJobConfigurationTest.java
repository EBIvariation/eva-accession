/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.configuration.jobs;

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
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.runner.CommandLineRunnerUtils;
import uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.rule.FixSpringMongoDbRule;

import javax.sql.DataSource;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATE_STUDY_SUBMITTED_VARIANTS_JOB;
import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@TestPropertySource("classpath:study-submitted-variants-test.properties")
public class DeprecateStudySubmittedVariantsJobConfigurationTest {

    private static final String TEST_DB = "test-db";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private DataSource datasource;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Test
    public void contextLoads() {

    }

    @Before
    public void setUp() throws Exception {
        this.mongoClient.dropDatabase(TEST_DB);
        MongoTestDatabaseSetup.populateTestDB(this.mongoTemplate);
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @After
    public void tearDown() {
        this.mongoClient.dropDatabase(TEST_DB);
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    public void basicJobCompletion() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        List<String> expectedSteps = Collections.singletonList(DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        MongoTestDatabaseSetup.assertPostDeprecationDatabaseState(this.mongoTemplate);
    }

    private void assertStepsExecuted(List expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

    @Test
    @DirtiesContext
    public void restartCompletedJobThatIsAlreadyInTheRepository() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        List<String> expectedSteps = Collections.singletonList(DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        MongoTestDatabaseSetup.assertPostDeprecationDatabaseState(this.mongoTemplate);

        long instanceIdFirstJob = CommandLineRunnerUtils.getLastJobExecution(DEPRECATE_STUDY_SUBMITTED_VARIANTS_JOB,
                                                                             jobExplorer,
                                                                             jobExecution.getJobParameters())
                                                        .getJobInstance().getInstanceId();

        jobExecution = jobLauncherTestUtils.launchJob();
        expectedSteps = Collections.singletonList(DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        long instanceIdSecondJob = CommandLineRunnerUtils.getLastJobExecution(DEPRECATE_STUDY_SUBMITTED_VARIANTS_JOB,
                                                                              jobExplorer,
                                                                              jobExecution.getJobParameters())
                                                         .getJobInstance().getInstanceId();
        assertNotEquals(instanceIdSecondJob, instanceIdFirstJob);
        MongoTestDatabaseSetup.assertPostDeprecationDatabaseState(this.mongoTemplate);
    }
}
