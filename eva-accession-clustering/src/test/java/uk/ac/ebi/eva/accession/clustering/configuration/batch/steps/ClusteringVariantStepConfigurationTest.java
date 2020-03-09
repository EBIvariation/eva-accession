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
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class ClusteringVariantStepConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @After
    public void tearDown() {
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
    }

    @Test
    @DirtiesContext
    @UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
    public void step() {
        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).count());
        assertTrue(allNotClustered());

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(CLUSTERING_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).count());
        assertTrue(allClustered());
    }

    private boolean allClustered() {
        DBCollection collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION);
        DBCursor dbObjects = collection.find();
        for (DBObject dbObject : dbObjects) {
            if (dbObject.get("rs") == null){
                return false;
            }
        }
        return true;
    }

    private boolean allNotClustered() {
        DBCollection collection = mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION);
        DBCursor dbObjects = collection.find();
        for (DBObject dbObject : dbObjects) {
            if (dbObject.get("rs") != null){
                return false;
            }
        }
        return true;
    }
}