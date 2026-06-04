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
 */
package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.DatabaseState;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.ContiguousIdBlocksDataSourceConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import javax.sql.DataSource;
import java.net.URI;
import java.sql.Connection;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_NEW_RS_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_SPLIT_OR_MERGED_RS_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_MERGE_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_SPLIT_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.STUDY_CLUSTERING_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATE_ID_PREFIX;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATE_ID_PREFIX;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_MONGO;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_RS_ACCESSION_RECOVERY;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_STUDY_FROM_MONGO;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        ContiguousIdBlocksDataSourceConfiguration.class})
@TestPropertySource("classpath:clustering-writer-test.properties")
public class ClusteringVariantJobConfigurationTest extends MongoTestContainerHelper {
    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongo;

    @Autowired
    @Qualifier(JOB_LAUNCHER_STUDY_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsStudyFromMongo;

    @Autowired
    @Qualifier(JOB_LAUNCHER_RS_ACCESSION_RECOVERY)
    private JobLauncherTestUtils jobLauncherTestUtilsMonotonicAccessionRecoveryAgent;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private InputParameters inputParameters;

    private MockRestServiceServer mockServer;

    @Autowired
    private CountServiceParameters countServiceParameters;

    @Autowired
    private DataSource dataSource;

    @Autowired
    @Qualifier("COUNT_STATS_REST_TEMPLATE")
    private RestTemplate restTemplate;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @BeforeEach
    public void init() throws Exception {
        mongoTemplate.getDb().drop();
        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS contiguous_id_blocks");
        }
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("test-data/contiguous_id_blocks_schema.sql"));
        populator.execute(dataSource);
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    public void jobFromMongo() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/submittedVariantEntityMongoReader.json");

        JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchJob();
        List<String> expectedSteps = new ArrayList<>();
        expectedSteps.add(CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        expectedSteps.add(PROCESS_RS_MERGE_CANDIDATES_STEP);
        expectedSteps.add(PROCESS_RS_SPLIT_CANDIDATES_STEP);
        expectedSteps.add(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP);
        expectedSteps.add(CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        expectedSteps.add(ACCESSIONING_SHUTDOWN_STEP);
        expectedSteps.add(BACK_PROPAGATE_NEW_RS_STEP);
        expectedSteps.add(BACK_PROPAGATE_SPLIT_OR_MERGED_RS_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    @DirtiesContext
    public void studyJobFromMongo() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/submittedVariantEntityStudyReader.json");

        JobExecution jobExecution = jobLauncherTestUtilsStudyFromMongo.launchJob();
        List<String> expectedSteps = new ArrayList<>();
        expectedSteps.add(STUDY_CLUSTERING_STEP);
        expectedSteps.add(ACCESSIONING_SHUTDOWN_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    @DirtiesContext
    public void testStudyJobFromMongoIdempotent() throws Exception {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/submittedVariantEntityStudyReader.json");

        jobLauncherTestUtilsStudyFromMongo.launchJob();
        DatabaseState databaseStateAfterFirstRun = DatabaseState.getCurrentDatabaseState(mongoTemplate);
        jobLauncherTestUtilsStudyFromMongo.launchJob();
        DatabaseState databaseStateAfterSecondRun = DatabaseState.getCurrentDatabaseState(mongoTemplate);
        assertEquals(databaseStateAfterFirstRun, databaseStateAfterSecondRun);
    }

    private void assertStepsExecuted(List<String> expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).
                filter(stepName -> !stepName.contains("dummyStep")).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

    @Test
    @DirtiesContext
    public void assertDataThatExceedsChunkSizeIsFullyProcessed() throws Exception {
        int numOperations = inputParameters.getChunkSize() * 3;
        createSplitCandidateEntriesThatExceedChunkSize(numOperations);
        createMergeCandidateEntriesThatExceedChunkSize(numOperations);
        JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchJob();
        assertEquals(numOperations,
                jobExecution.getStepExecutions()
                        .stream()
                        .filter(stepExecution -> stepExecution.getStepName()
                                .equals(PROCESS_RS_SPLIT_CANDIDATES_STEP))
                        .findFirst().get().getReadCount());
        assertEquals(numOperations,
                jobExecution.getStepExecutions()
                        .stream()
                        .filter(stepExecution -> stepExecution.getStepName()
                                .equals(PROCESS_RS_MERGE_CANDIDATES_STEP))
                        .findFirst().get().getReadCount());
    }

    @Test
    @DirtiesContext
    public void testJobRSAccessionRecovery() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtilsMonotonicAccessionRecoveryAgent.launchJob();
        List<String> expectedSteps = new ArrayList<>();
        expectedSteps.add(RS_ACCESSION_RECOVERY_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    private void createMergeCandidateEntriesThatExceedChunkSize(int numSplitCandidateOperations) {
        //Candidates for merge are entries with same locus but different RS
        SubmittedVariantInactiveEntity ss1 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(1L, 1L, 100L, "C", "T"));
        SubmittedVariantInactiveEntity ss2 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(2L, 2L, 100L, "C", "A"));
        for (int i = 0; i < numSplitCandidateOperations; i++) {
            SubmittedVariantOperationEntity mergeOperation = new SubmittedVariantOperationEntity();
            mergeOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATES_EVENT_TYPE,
                    ss1.getAccession(), null, "Mock merge candidate",
                    Arrays.asList(ss1, ss2));
            mergeOperation.setId(MERGE_CANDIDATE_ID_PREFIX + "_" + ss1.getReferenceSequenceAccession() + "_" + i);
            mongoTemplate.insert(mergeOperation,
                    mongoTemplate.getCollectionName(SubmittedVariantOperationEntity.class));
        }
    }

    private void createSplitCandidateEntriesThatExceedChunkSize(int numSplitCandidateOperations) {
        //Candidates for split are entries with same RS but different locus
        SubmittedVariantInactiveEntity ss3 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(3L, 3L, 100L, "C", "T"));
        SubmittedVariantInactiveEntity ss4 = new SubmittedVariantInactiveEntity(
                createSSWithLocus(4L, 3L, 101L, "G", "A"));
        for (int i = 0; i < numSplitCandidateOperations; i++) {
            SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
            splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                    ss3.getAccession(), null, "Mock split candidate",
                    Arrays.asList(ss3, ss4));
            splitOperation.setId(SPLIT_CANDIDATE_ID_PREFIX + "_" + ss3.getReferenceSequenceAccession() + "_" + i);
            mongoTemplate.insert(splitOperation,
                    mongoTemplate.getCollectionName(SubmittedVariantOperationEntity.class));
        }
    }

    private SubmittedVariantEntity createSSWithLocus(Long ssAccession, Long rsAccession, Long start, String reference,
                                                     String alternate) {
        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession, inputParameters.getAssemblyAccession(),
                60711,
                "PRJ1", "chr1", start, reference, alternate, rsAccession, false, false, false,
                false, 1);
    }

}