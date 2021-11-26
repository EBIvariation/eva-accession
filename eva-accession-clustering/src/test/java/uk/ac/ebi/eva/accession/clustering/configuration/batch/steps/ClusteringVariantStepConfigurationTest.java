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

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import org.bson.Document;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
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
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
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
@TestPropertySource("classpath:clustering-issuance-test.properties")
@UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
public class ClusteringVariantStepConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final String CLUSTERED_VARIANT_COLLECTION = "clusteredVariantEntity";

    private static final String DBSNP_CLUSTERED_VARIANT_COLLECTION = "dbsnpClusteredVariantEntity";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    public static final String ASSEMBLY = "GCA_000000001.1";

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_VCF)
    private JobLauncherTestUtils jobLauncherTestUtilsFromVcf;

    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_MONGO)
    private JobLauncherTestUtils jobLauncherTestUtilsFromMongo;

    @Autowired
    private MongoTemplate mongoTemplate;

    private MockRestServiceServer mockServer;

    @Autowired
    private CountServiceParameters countServiceParameters;

    @Autowired
    private RestTemplate restTemplate;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void init() throws Exception {
        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    /**
     * Note how this test generates 1 less clusteredVariantAccession than the stepFromMongo test, because
     * the VCF here already provides a dbsnp RS for one submitted variant.
     * <p>
     * Note also how we are not checking that all the variants end up clustered because it's assumed that
     * the SS already links to the dbsnp RS (although it's not in the DB test data). The assumption will hold in real
     * data because if it linked to another RS, then it would be merged (see tests in
     * MergeAccessionClusteringWriterTest), and if it didn't link to any RS, then it should not
     * be updated because it hasn't been provided as input to the clustering pipeline.
     */
    @Ignore
    @Test
    @DirtiesContext
    // TODO: Re-visit during EVA-2611
    public void stepFromVcf() {
        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        insertClusteredVariant();
        assertEquals(1, mongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_COLLECTION).countDocuments());
        assertTrue(allSubmittedVariantsNotClustered());

        JobExecution jobExecution = jobLauncherTestUtilsFromVcf.launchStep(CLUSTERING_FROM_VCF_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        assertClusteredVariantsCreated(Arrays.asList(3000000000L, 3000000001L, 3000000002L));
        assertGeneratedAccessions(SUBMITTED_VARIANT_COLLECTION, "rs",
                                  Arrays.asList(-1L, 3000000000L, 3000000000L, 3000000001L, 3000000002L));
    }

    private void insertClusteredVariant() {
        ClusteredVariant variant = new ClusteredVariant(ASSEMBLY, 9999, "1", 3000, VariantType.SNV, false, null);
        Function<IClusteredVariant, String> summaryFunction = new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariantEntity entity = new ClusteredVariantEntity(30L, summaryFunction.apply(variant), variant);
        mongoTemplate.insert(entity, DBSNP_CLUSTERED_VARIANT_COLLECTION);
    }

    @Test
    @DirtiesContext
    public void nonClusteredVariantStepFromMongo() {
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
    @UsingDataSet(locations = {"/test-data/submittedVariantEntityMongoReader.json",
            "/test-data/clusteredVariantEntityMongoReader.json"})
    public void clusteredVariantStepFromMongo() {
        assertEquals(6, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
        assertEquals(1, getSubmittedVariantsWithRS());

        JobExecution jobExecution = jobLauncherTestUtilsFromMongo.launchStep(
                CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        // Clustered variants from Mongo step only collects RS merge or split candidates
        // which are later processed by RS Merge and Split writers - consult tests for those writers for post-merge scenarios
        List<SubmittedVariantOperationEntity> operations = mongoTemplate.findAll(SubmittedVariantOperationEntity.class);
        assertEquals(1, operations.size());
        assertEquals(EventType.RS_MERGE_CANDIDATES, operations.get(0).getEventType());

        // TODO test retries (this step only, for now at least)
    }

    private int getSubmittedVariantsWithRS() {
        int count = 0;
        MongoCollection<Document> collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION);
        FindIterable<Document> documents = collection.find();
        for (Document document : documents) {
            if (document.get("rs") != null) {
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