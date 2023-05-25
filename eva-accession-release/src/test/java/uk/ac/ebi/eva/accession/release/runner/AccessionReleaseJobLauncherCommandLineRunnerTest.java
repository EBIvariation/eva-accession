package uk.ac.ebi.eva.accession.release.runner;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;

import com.mongodb.client.result.UpdateResult;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.JobInstance;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.runner.CommandLineRunnerUtils;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;

import javax.sql.DataSource;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACCESSION_RELEASE_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.runner.AccessionReleaseJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS;
import static uk.ac.ebi.eva.accession.release.runner.AccessionReleaseJobLauncherCommandLineRunner.EXIT_WITH_ERRORS;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantOperationEntity.json"})
@TestPropertySource("classpath:release-pipeline-test.properties")
@SpringBatchTest
public class AccessionReleaseJobLauncherCommandLineRunnerTest {
    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DataSource datasource;

    @Autowired
    private AccessionReleaseJobLauncherCommandLineRunner runner;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private static final String TEST_DB = "test-db";

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Test
    public void contextLoads() {

    }

    @Before
    public void setUp() throws Exception {
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(ACCESSION_RELEASE_JOB);
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
        remediateMongoClusteredVariantCollection();
        remediateMongoClusteredVariantOperationCollection();
    }

    @After
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
        inputParameters.setForceRestart(false);
    }

    @Test
    @DirtiesContext
    public void runSuccessfulJob() throws Exception {
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
        injectErrorIntoMongoClusteredVariantCollection();
        // (batch size is 5 and the forced error was at entry#7 (when sorted by contig + start) for the GCA_000409795.2 assembly)
        // Two variants in batch#1 are excluded for the following reasons - leaving us with only 3 processed
        // Variant AbstractVariant{chromosome='CM001954.1', position=80-81, reference='Y', alternate='YT', ids='[rs8181]'} excluded (it has non-nucleotide letters and it's not a named variant)
        // Variant coordinate 69999999 greater than end of chromosome NC_023654.1: 720. AbstractVariant{chromosome='CM001954.1', position=70000000-70000000, reference='A', alternate='', ids='[rs100]'}
        JobInstance failingJobInstance = runJobAandCheckResults(RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP, 3);

        remediateMongoClusteredVariantCollection();
        inputParameters.setForceRestart(true);
        runJobBAndCheckRestart(failingJobInstance);
    }

    private JobInstance runJobAandCheckResults(String stepName, int expectedResultSize) throws Exception {
        runner.run();
        assertEquals(EXIT_WITH_ERRORS, runner.getExitCode());
        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(ACCESSION_RELEASE_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution = jobRepository.getLastStepExecution(currentJobInstance, stepName);

        assertEquals(expectedResultSize, stepExecution.getWriteCount());

        return currentJobInstance;
    }

    private void runJobBAndCheckRestart(JobInstance failingJobInstance) throws Exception {
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());

        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(ACCESSION_RELEASE_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution1 = jobRepository.getLastStepExecution(currentJobInstance,
                                                                          RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP);
        StepExecution stepExecution2 = jobRepository.getLastStepExecution(currentJobInstance,
                                                                          RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP);
        StepExecution stepExecution3 = jobRepository.getLastStepExecution(currentJobInstance,
                                                                          RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP);
        StepExecution stepExecution4 =
                jobRepository.getLastStepExecution(currentJobInstance,
                                                   RELEASE_DBSNP_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP);
        assertNotEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());
        // 5 active + 5 merged + 3 deprecated + 2 merged deprecated
        assertEquals(15, stepExecution1.getWriteCount() + stepExecution2.getWriteCount() +
                stepExecution3.getWriteCount() + stepExecution4.getWriteCount());
    }

    @Test
    @DirtiesContext
    public void resumeFailingJob() throws Exception {
        // Inject error to make an error appear in the Deprecated variant release step
        injectErrorIntoMongoClusteredVariantOperationCollection();
        JobInstance failingJobInstance = runJobAandCheckResults(RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP, 0);

        remediateMongoClusteredVariantOperationCollection();
        // Ensure resumption from deprecated variant step
        runJobBAndCheckResume(failingJobInstance);
    }

    private void runJobBAndCheckResume(JobInstance failingJobInstance) throws Exception {
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());

        JobInstance currentJobInstance = CommandLineRunnerUtils.getLastJobExecution(ACCESSION_RELEASE_JOB,
                                                                                    jobExplorer,
                                                                                    inputParameters.toJobParameters())
                                                               .getJobInstance();
        StepExecution stepExecution1 = jobRepository.getLastStepExecution(currentJobInstance,
                                                                          RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP);
        StepExecution stepExecution2 =
                jobRepository.getLastStepExecution(currentJobInstance,
                                                   RELEASE_DBSNP_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP);
        assertEquals(failingJobInstance.getInstanceId(), currentJobInstance.getInstanceId());
        // Resume from deprecated variants step - 3 deprecated + 2 merged deprecated
        assertEquals(5, stepExecution1.getWriteCount() + stepExecution2.getWriteCount());
    }

    private void injectErrorIntoMongoClusteredVariantOperationCollection() throws Exception {
        Query query = query(where("_id").is("5c519550dcc3abf7ee9125af"));
        Update update = new Update();
        // Intentionally inject error in the Mongo collection
        update.set("inactiveObjects.0.start", "8421515jibberish");
        mongoTemplate.updateFirst(query, update, DbsnpClusteredVariantOperationEntity.class);
    }

    private void remediateMongoClusteredVariantOperationCollection() throws Exception {
        Query query = query(where("_id").is("5c519550dcc3abf7ee9125af"));
        Update update = new Update();
        // Intentionally inject error in the Mongo collection
        update.set("inactiveObjects.0.start", 8421515L);
        mongoTemplate.updateFirst(query, update, DbsnpClusteredVariantOperationEntity.class);
    }

    private void injectErrorIntoMongoClusteredVariantCollection() throws Exception {
        Query query = query(where("_id").is("F475F4F6657CF52CE8BF8416AA945EF669CB1BCD"));
        Update update = new Update();
        // Intentionally inject error in entry#7 (contig, start sorted) in the Mongo collection
        update.set("start", "5jibberish");
        UpdateResult result = mongoTemplate.updateFirst(query, update, DbsnpClusteredVariantEntity.class);
    }

    private void remediateMongoClusteredVariantCollection() throws Exception {
        Query query = query(where("_id").is("F475F4F6657CF52CE8BF8416AA945EF669CB1BCD"));
        Update update = new Update();
        // Remediate error in entry#7 in the Mongo collection
        update.set("start", 5L);
        UpdateResult result = mongoTemplate.updateFirst(query, update, DbsnpClusteredVariantEntity.class);
    }

    @Test
    @DirtiesContext
    public void forceRestartButNoJobInTheRepository() throws Exception {
        inputParameters.setForceRestart(true);
        assertEquals(Collections.EMPTY_LIST, jobExplorer.getJobNames());
        runner.run();

        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

}
