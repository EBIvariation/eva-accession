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

package uk.ac.ebi.eva.accession.remapping.batch.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.remapping.test.configuration.MongoTestConfiguration;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/submittedVariantEntity.json"
})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class EvaSubmittedVariantMongoReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000409795.2";

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private EvaSubmittedVariantMongoReader reader;

    @Before
    public void setUp() {
        ExecutionContext executionContext = new ExecutionContext();
        reader = new EvaSubmittedVariantMongoReader(ASSEMBLY, mongoTemplate);
        reader.open(executionContext);
    }

    @After
    public void tearDown() {
        reader.close();
    }

    @Test
    public void basicRead() throws Exception {
        List<SubmittedVariantEntity> variants = readIntoList(reader);
        assertEquals(1, variants.size());

        for (SubmittedVariantEntity variant : variants) {
            assertEquals(ASSEMBLY, variant.getReferenceSequenceAccession());
        }
    }

    private <T> List<T> readIntoList(ItemReader<T> reader) throws Exception {
        List<T> variants = new ArrayList<>();
        T variant;

        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }

        return variants;
    }

}
