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

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
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
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.DatabaseState;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import java.net.URI;
import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.*;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.MERGE_CANDIDATE_ID_PREFIX;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATE_ID_PREFIX;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.*;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-writer-test.properties")
public class ResolveMergeThenSplitCandidatesJobConfigurationTest {

    private static final String TEST_DB = "test-db";

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_VCF)
    private JobLauncherTestUtils jobLauncherTestUtilsFromVcf;

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongo;

    @Autowired
    @Qualifier(JOB_LAUNCHER_STUDY_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsStudyFromMongo;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private InputParameters inputParameters;

    private MockRestServiceServer mockServer;

    @Autowired
    private CountServiceParameters countServiceParameters;

    @Autowired
    @Qualifier("COUNT_STATS_REST_TEMPLATE")
    private RestTemplate restTemplate;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void init() throws Exception {
        mongoTemplate.getDb().drop();
        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }


    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/submittedVariantOperationEntityMongoReader.json.json"})
    public void jobFromMongo() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchJob();
        List<String> expectedSteps = new ArrayList<>();
        expectedSteps.add(PROCESS_RS_MERGE_CANDIDATES_STEP);
        expectedSteps.add(PROCESS_RS_SPLIT_CANDIDATES_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    private void assertStepsExecuted(List<String> expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).
                filter(stepName -> !stepName.contains("dummyStep")).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

}