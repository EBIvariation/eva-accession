/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.release.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;
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
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;


@RunWith(SpringRunner.class)
@TestPropertySource("classpath:accession-release-test.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class AccessionedVariantMongoReaderTest {

    private static final String ASSEMBLY_ACCESSION_1 = "GCF_000409795.2";

    private static final String ASSEMBLY_ACCESSION_2 = "GCF_000001735.3";

    private static final String TEST_DB = "test-db";

    public static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String RS_1 = "869808637";

    private static final String RS_2 = "869927931";

    private static final String RS_3 = "347048227";

    private AccessionedVariantMongoReader reader;

    private ExecutionContext executionContext;

    @Autowired
    private MongoClient mongoClient;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() throws Exception {
        executionContext = new ExecutionContext();
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_1, mongoClient, TEST_DB);
    }

    @Test
    public void readTestDataMongo() {
        MongoClient mongoClient = new MongoClient(new ServerAddress("localhost", 27017));
        MongoDatabase db = mongoClient.getDatabase(TEST_DB);
        MongoCollection<Document> collection = db.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY);

        AggregateIterable<Document> result = collection.aggregate(reader.buildAggregation())
                                                       .allowDiskUse(true)
                                                       .useCursor(true);

        MongoCursor<Document> cursor = result.iterator();

        List<Variant> variants = new ArrayList<>();
        while (cursor.hasNext()) {
            Document clusteredVariant = cursor.next();
            Variant variant = reader.getVariant(clusteredVariant);
            variants.add(variant);
        }
        assertEquals(2, variants.size());
     }

    @Test
    public void reader() throws Exception {
        reader.open(executionContext);
        List<Variant> variants = new ArrayList<>();
        Variant variant;
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        reader.close();
        assertEquals(2, variants.size());
    }

    @Test
    public void linkedSubmittedVariants() throws Exception {
        reader.open(executionContext);
        Map<String, Variant> variants = new HashMap<>();
        Variant variant;
        while ((variant = reader.read()) != null) {
            variants.put(variant.getMainId(), variant);
        }
        reader.close();
        assertEquals(2, variants.size());
        assertEquals(2, variants.get(RS_1).getSourceEntries().size());
        assertEquals(1, variants.get(RS_2).getSourceEntries().size());
    }

    @Test
    public void queryOtherAssembly() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_2, mongoClient, TEST_DB);
        reader.open(executionContext);
        Map<String, Variant> variants = new HashMap<>();
        Variant variant;
        while ((variant = reader.read()) != null) {
            variants.put(variant.getMainId(), variant);
        }
        assertEquals(1, variants.size());
        assertEquals(2, variants.get(RS_3).getSourceEntries().size());
    }
}
