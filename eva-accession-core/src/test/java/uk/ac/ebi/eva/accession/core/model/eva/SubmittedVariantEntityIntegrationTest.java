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
package uk.ac.ebi.eva.accession.core.model.eva;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;

import java.util.Collections;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {MongoConfiguration.class})
@TestPropertySource("classpath:test-model.properties")
public class SubmittedVariantEntityIntegrationTest {

    private static final String TEST_DB = "test-db";

    private Function<ISubmittedVariant, String> submittedHashingFunction;

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
        submittedHashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void variantWithMapWeightStoredInMongo() {
        assertMapWeight(2, 2);
    }

    @Test
    public void variantNoMapWeightStoredInMongo() {
        assertMapWeight(null, null);
    }

    /**
     * Mapping weight of 1 should be stored as null in mongo
     */
    @Test
    public void mapWeightOneStoredAsNullInMongo() {
        assertMapWeight(null, 1);
    }

    private void assertMapWeight(Integer expectedMapWeightFromMongo, Integer mapWeight) {
        SubmittedVariantEntity ss = createSubmittedVariantEntity(mapWeight);
        mongoTemplate.insert(Collections.singletonList(ss), SubmittedVariantEntity.class);

        SubmittedVariantEntity variantEntity = mongoTemplate.find(new Query(), SubmittedVariantEntity.class).get(0);
        assertEquals(expectedMapWeightFromMongo, variantEntity.getMapWeight());
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(Integer mapWeight) {
        SubmittedVariant variant = createSubmittedVariant();
        String hash = submittedHashingFunction.apply(variant);
        SubmittedVariantEntity variantEntity = new SubmittedVariantEntity(5000000000L, hash, "asm", 1000, "project",
                                                                          "contig", 100, "A", "T", null, false, false,
                                                                          false, false, 1, mapWeight);
        return variantEntity;
    }

    private SubmittedVariant createSubmittedVariant() {
        return new SubmittedVariant("asm", 1000, "project", "contig", 100, "A", "T", null);
    }
}
