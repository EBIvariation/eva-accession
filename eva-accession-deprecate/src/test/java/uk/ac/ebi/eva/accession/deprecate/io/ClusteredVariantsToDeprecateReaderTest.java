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
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.rule.FixSpringMongoDbRule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpClusteredVariantEntityDeclustered.json"})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class ClusteredVariantsToDeprecateReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ID_1 = "CA98EA5284F0BF46727CEF3373EE7144839172F5";

    private static final Long ACCESSION_1 = 853683894L;

    private ExecutionContext executionContext;

    private ClusteredVariantsToDeprecateReader reader;

    @Autowired
    private MongoClient mongoClient;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() {
        executionContext = new ExecutionContext();
        reader = new ClusteredVariantsToDeprecateReader(mongoClient, TEST_DB);
    }

    @Test
    public void ReadVariantsToDeprecate() {
        List<DbsnpClusteredVariantEntity> variants = readIntoList();
        assertEquals(1, variants.size());
        assertEquals(ID_1, variants.get(0).getId());
        assertEquals(ACCESSION_1, variants.get(0).getAccession());
    }

    private List<DbsnpClusteredVariantEntity> readIntoList() {
        reader.open(executionContext);
        DbsnpClusteredVariantEntity variant;
        List<DbsnpClusteredVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        reader.close();
        return variants;
    }

}