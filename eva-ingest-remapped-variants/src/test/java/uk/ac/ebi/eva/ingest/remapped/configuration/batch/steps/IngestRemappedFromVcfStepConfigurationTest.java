/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.ingest.remapped.configuration.batch.steps;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
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

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.ingest.remapped.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.ingest.remapped.test.rule.FixSpringMongoDbRule;

import static junit.framework.TestCase.assertEquals;
import static uk.ac.ebi.eva.ingest.remapped.configuration.BeanNames.INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:ingest-remapped-variants.properties")
@UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
public class IngestRemappedFromVcfStepConfigurationTest {

    private static final String TEST_DB = "test-ingest-remapping";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

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
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    public void runStep() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP);
        Assert.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());
    }
}