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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.steps;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
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
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.ContiguousIdBlocksDataSourceConfiguration;
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
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.STUDY_CLUSTERING_STEP;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_MONGO;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_MONGO_ONLY_FIRST_STEP;
import static uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_STUDY_FROM_MONGO;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        ContiguousIdBlocksDataSourceConfiguration.class})
@TestPropertySource("classpath:clustering-issuance-test.properties")
public class ClusteringVariantStepConfigurationTest extends MongoTestContainerHelper {
    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongo;

    @Autowired
    @Qualifier(JOB_LAUNCHER_STUDY_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsStudyFromMongo;

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO_ONLY_FIRST_STEP)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongoOnlyFirstStep;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private ResourceLoader resourceLoader;

    private MockRestServiceServer mockServer;

    @Autowired
    private CountServiceParameters countServiceParameters;

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
    public void nonClusteredVariantStepFromMongo() {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/submittedVariantEntity.json");

        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        assertTrue(allSubmittedVariantsNotClustered());

        JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchStep(
                CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        assertClusteredVariantsCreated(Arrays.asList(3000000000L, 3000000001L, 3000000002L, 3000000003L));
        assertSubmittedVariantsUpdated();
    }

    @Test
    @DirtiesContext
    public void clusteredVariantStepStudyFromMongo() {
        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/submittedVariantEntityStudyReader.json");

        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        assertEquals(2, getSubmittedVariantsWithRSOrBackPropRS());

        JobExecution jobExecution = jobLauncherTestUtilsStudyFromMongo.launchStep(STUDY_CLUSTERING_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        assertClusteredVariantsCreated(Arrays.asList(3000000000L));
        assertEquals(4, getSubmittedVariantsWithRSOrBackPropRS());
    }

    @Test
    @DirtiesContext
    public void clusteredVariantStepFromMongo() throws Exception {
        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load("/test-data/submittedVariantEntityMongoReader.json");
        mongoTestDataLoader.load("/test-data/clusteredVariantEntityMongoReader.json");

        assertEquals(6, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        assertEquals(1, getSubmittedVariantsWithRSOrBackPropRS());

        // Due to a bug in launching individual steps from a flow - https://github.com/spring-projects/spring-batch/issues/1311
        // the following cannot be executed directly, therefore we launch the entire job and ensure that the
        // CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP step was completed
        //JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchStep(
        //CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        JobExecution jobExecution = jobLauncherTestUtilsFromMongoOnlyFirstStep.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertTrue(jobExecution.getStepExecutions().stream().map(StepExecution::getStepName)
                .anyMatch(s -> s.equals(CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP)));

        // Clustered variants from Mongo step only collects RS merge or split candidates
        // which are later processed by RS Merge and Split writers - consult tests for those writers for post-merge scenarios
        List<SubmittedVariantOperationEntity> operations = mongoTemplate.findAll(SubmittedVariantOperationEntity.class);
        assertEquals(1, operations.size());
        assertEquals(EventType.RS_MERGE_CANDIDATES, operations.get(0).getEventType());
    }

    private int getSubmittedVariantsWithRSOrBackPropRS() {
        int count = 0;
        MongoCollection<Document> collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION);
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (document.get("rs") != null || document.get("backPropRS") != null) {
                count++;
            }
        }
        return count;
    }

    private boolean allSubmittedVariantsNotClustered() {
        MongoCollection<Document> collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION);
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (document.get("rs") != null) {
                return false;
            }
        }
        return true;
    }

    private void assertClusteredVariantsCreated(List<Long> expectedAccessions) {
        MongoCollection<Document> collection = mongoTemplate.getCollection(CLUSTERED_VARIANT_COLLECTION);
        assertEquals(expectedAccessions.size(), collection.countDocuments());
        assertGeneratedAccessions(CLUSTERED_VARIANT_COLLECTION, "accession", expectedAccessions);
    }

    private void assertGeneratedAccessions(String collectionName, String accessionField,
                                           List<Long> expectedAccessions) {
        List<Long> generatedAccessions = new ArrayList<>();
        MongoCollection<Document> collection = mongoTemplate.getCollection(collectionName);
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            Long accessionId = (Long) document.get(accessionField);
            generatedAccessions.add(accessionId == null ? -1 : accessionId);
        }
        Collections.sort(generatedAccessions);
        assertEquals(expectedAccessions, generatedAccessions);
    }

    private void assertSubmittedVariantsUpdated() {
        assertTrue(allSubmittedVariantsClustered());
        List<Long> expectedAccessions = Arrays.asList(3000000000L, 3000000000L, 3000000001L, 3000000002L, 3000000003L);
        assertGeneratedAccessions(SUBMITTED_VARIANT_COLLECTION, "rs", expectedAccessions);
    }

    private boolean allSubmittedVariantsClustered() {
        MongoCollection<Document> collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION);
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (document.get("rs") == null) {
                return false;
            }
        }
        return true;
    }
}