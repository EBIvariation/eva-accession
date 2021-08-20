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
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
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
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.clustering.parameters.CountParameters;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_VCF_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_MONGO;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_VCF;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class ClusteringVariantJobConfigurationTest {

    private static final String TEST_DB = "test-db";

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_VCF)
    private JobLauncherTestUtils jobLauncherTestUtilsFromVcf;

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongo;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private RestTemplate restTemplate;

    private MockRestServiceServer mockServer;

    @Autowired
    private CountParameters countParameters;
    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void init() throws Exception {
        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/clusteredVariantEntityForVcfJob.json"})
    public void jobFromVcf() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtilsFromVcf.launchJob();
        List<String> expectedSteps = Collections.singletonList(CLUSTERING_FROM_VCF_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    @DirtiesContext
    public void jobFromMongo() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchJob();
        List<String> expectedSteps = new ArrayList<>();
        expectedSteps.add(CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        expectedSteps.add(CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    private DbsnpClusteredVariantEntity createClusteredVariantEntity() {
        ClusteredVariant variant = new ClusteredVariant("GCA_000000001.1", 1000, "1", 3000, VariantType.SNV, false,
                                                        null);
        Function<IClusteredVariant, String> clusteredHashingFunction =
                new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        String hash = clusteredHashingFunction.apply(variant);
        DbsnpClusteredVariantEntity variantEntity = new DbsnpClusteredVariantEntity(30L, hash, variant, 1);
        return variantEntity;
    }

    private void assertStepsExecuted(List<String> expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }
}