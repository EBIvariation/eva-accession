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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
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
import uk.ac.ebi.eva.accession.clustering.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:clustering-pipeline-test.properties")
@UsingDataSet(locations = {"/test-data/submittedVariantEntityMongoReader.json"})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class ClusteringMongoReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final int CHUNK_SIZE = 5;

    private static final String SUBMITTED_VARIANT_ENTITY = "submittedVariantEntity";

    // from the json input data. the hash can be computed in bash too:
    // echo -n "GCA_000000001.1_projectId_1_2_3000_T_G" | sha1sum | awk '{ print toupper($1) }'
    private static final String CLUSTERED_SUBMITTED_VARIANT_ID = "C195245DADAA13BB00474F66A57A21718B332B5A";

    private static final String NOT_CLUSTERED_SUBMITTED_VARIANT_ID = "96A7CDAE49D1ACDC833524E294C37BDC8F8435FB";

    private ClusteringMongoReader reader;

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
    public void setUp() throws Exception {
        ExecutionContext executionContext = new ExecutionContext();
        reader = new ClusteringMongoReader(mongoClient, TEST_DB, mongoTemplate, ASSEMBLY, CHUNK_SIZE);
        reader.open(executionContext);
    }

    @After
    public void tearDown() {
        reader.close();
        mongoClient.dropDatabase(TEST_DB);
    }

    @Test
    public void readAllSubmittedVariants() {
        assertEquals(6, mongoTemplate.getCollection(SUBMITTED_VARIANT_ENTITY).countDocuments());
        List<SubmittedVariantEntity> variants = readIntoList();
        assertEquals(6, variants.size());
        assertTrue(variants.stream().anyMatch(x -> Objects.equals(x.getId(), CLUSTERED_SUBMITTED_VARIANT_ID)));
        assertTrue(variants.stream().anyMatch(x -> Objects.equals(x.getId(), NOT_CLUSTERED_SUBMITTED_VARIANT_ID)));
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