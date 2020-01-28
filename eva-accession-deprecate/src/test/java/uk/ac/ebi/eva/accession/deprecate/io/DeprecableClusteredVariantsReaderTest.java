/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.io;

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

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.rule.FixSpringMongoDbRule;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/dbsnpClusteredVariantEntityDeclustered.json"})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class DeprecableClusteredVariantsReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ID_1 = "BCAB105FD3C0108A54354BB6B661C3146C874F4B";

    private static final String ID_2 = "E353FC48E7563BB79DCE4D6A2046FCE07DB17AC8";

    private static final String ASM_1 = "GCA_000000001.1";

    private static final String ASM_2 = "GCA_000000002.1";

    private static final String ASM_3 = "GCA_000000003.1";

    private static final String ASM_4 = "GCA_000000004.1";

    private static final int CHUNK_SIZE = 5;

    private ExecutionContext executionContext;

    private DeprecableClusteredVariantsReader reader;

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
        executionContext = new ExecutionContext();
        reader = new DeprecableClusteredVariantsReader(mongoClient, TEST_DB, mongoTemplate, CHUNK_SIZE);
        reader.open(executionContext);
    }

    @After
    public void tearDown() {
        reader.close();
        mongoClient.dropDatabase(TEST_DB);
    }

    @Test
    public void readDeprecateClusteredVariants() {
        List<DbsnpClusteredVariantEntity> variants = readIntoList();
        assertEquals(5, variants.size());
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_1)));
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_2)));
    }

    private List<DbsnpClusteredVariantEntity> readIntoList() {
        DbsnpClusteredVariantEntity variant;
        List<DbsnpClusteredVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }

    @Test
    public void readSubsetOfAssemblies() {
        reader = new DeprecableClusteredVariantsReader(mongoClient, TEST_DB, mongoTemplate,
                                                       Arrays.asList(ASM_2, ASM_3), CHUNK_SIZE);
        reader.open(executionContext);
        List<DbsnpClusteredVariantEntity> variants = readIntoList();
        assertEquals(1, variants.size());

        // Not present because it was not listed to the reader and its variants shouldn't be deprecated
        assertFalse(variants.stream().anyMatch(x -> x.getAssemblyAccession().equals(ASM_1)));

        // Not present because its variant shouldn't be deprecated
        assertFalse(variants.stream().anyMatch(x -> x.getAssemblyAccession().equals(ASM_2)));

        // Present
        assertTrue(variants.stream().anyMatch(x -> x.getAssemblyAccession().equals(ASM_3)));

        // Not present because it was not listed to the reader
        assertFalse(variants.stream().anyMatch(x -> x.getAssemblyAccession().equals(ASM_4)));
    }
}
