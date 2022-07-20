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
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:clustering-pipeline-test.properties")
@UsingDataSet(locations = {"/test-data/submittedVariantEntityStudyReader.json",
        "/test-data/dbsnpSubmittedVariantEntityMongoReader.json"})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class StudyClusteringMongoReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final List<String> PROJECTS = Arrays.asList("projectId_2", "projectId_3");

    private static final int CHUNK_SIZE = 5;

    private static final String SUBMITTED_VARIANT_ENTITY = "submittedVariantEntity";

    private static final String PROJECT_1_HASH = "96A7CDAE49D1ACDC833524E294C37BDC8F8435FB";

    private static final String PROJECT_2_HASH = "9F05088C2058BC2AECFF8B904E439E2FD4C67F20";

    private static final String PROJECT_3_HASH = "9B9240E0488CA3960B2FFC161878DA7F21FC1756";

    private StudyClusteringMongoReader studyClusteringMongoReader;

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
    public void setUp(){
        ExecutionContext executionContext = new ExecutionContext();
        studyClusteringMongoReader = new StudyClusteringMongoReader(mongoTemplate, ASSEMBLY, PROJECTS, CHUNK_SIZE);
        studyClusteringMongoReader.open(executionContext);
    }

    @After
    public void tearDown() {
        studyClusteringMongoReader.close();
        mongoClient.dropDatabase(TEST_DB);
    }

    @Test
    public void readSubmittedVariantsPerStudy() {
        assertEquals(3, mongoTemplate.getCollection(SUBMITTED_VARIANT_ENTITY).countDocuments());
        List<SubmittedVariantEntity> variants = readIntoList(studyClusteringMongoReader);
        assertEquals(2, variants.size());

        Set<String> hashes = variants.stream().map(SubmittedVariantEntity::getId).collect(Collectors.toSet());
        assertTrue(hashes.contains(PROJECT_2_HASH));
        assertTrue(hashes.contains(PROJECT_3_HASH));
        assertFalse(hashes.contains(PROJECT_1_HASH));
    }

    private List<SubmittedVariantEntity> readIntoList(StudyClusteringMongoReader reader) {
        SubmittedVariantEntity variant;
        List<SubmittedVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }
}