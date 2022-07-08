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
package uk.ac.ebi.eva.accession.deprecate.batch.io;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.rule.FixSpringMongoDbRule;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup.ASSEMBLY;
import static uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup.populateTestDB;
import static uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup.STUDY1;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:study-submitted-variants-test.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class StudySubmittedVariantsReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ID_1 = "hash5";

    private static final String ID_2 = "hash6";

    private static final String ID_3 = "hash7";

    private static final int CHUNK_SIZE = 5;

    private ExecutionContext executionContext;

    private StudySubmittedVariantsReader reader;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() {
        populateTestDB(this.mongoTemplate);
        executionContext = new ExecutionContext();
        reader = new StudySubmittedVariantsReader(ASSEMBLY, STUDY1, mongoTemplate, CHUNK_SIZE);
        reader.open(executionContext);
    }

    @After
    public void tearDown() {
        reader.close();
        mongoClient.dropDatabase(TEST_DB);
    }

    @Test
    public void readStudySubmittedVariants() {
        List<SubmittedVariantEntity> variants = readIntoList();
     assertEquals(3, variants.size());
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_1)));
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_2)));
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_3)));
    }

    private List<SubmittedVariantEntity> readIntoList() {
        SubmittedVariantEntity variant;
        List<SubmittedVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }
}
