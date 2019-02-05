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

package uk.ac.ebi.eva.accession.release.io;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
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
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.util.CloseableIterator;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.MERGED_INTO_KEY;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"
})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class MergedVariantMongoReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    private static final String ASSEMBLY = "GCF_000409795.2";

    private static final String ID_1_A = "CM001954.1_5_G_A";

    private static final String ID_1_T = "CM001954.1_5_G_T";

    private static final String ID_1_MERGED_INTO = "rs869808637";

    private static final String ID_2 = "CM001941.2_13_T_G";

    private static final String ID_2_MERGED_INTO = "rs869927931";

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

    private MergedVariantMongoReader reader;

    private ExecutionContext executionContext;

    @Before
    public void setUp() throws Exception {
        executionContext = new ExecutionContext();
        reader = new MergedVariantMongoReader(ASSEMBLY, mongoClient, TEST_DB);
    }

    @Test
    public void readMergedVariants() {
        MongoDatabase db = mongoClient.getDatabase(TEST_DB);
        MongoCollection<Document> collection = db.getCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY);

        AggregateIterable<Document> result = collection.aggregate(reader.buildAggregation())
                                                       .allowDiskUse(true)
                                                       .useCursor(true);
        MongoCursor<Document> iterator = result.iterator();
        List<Variant> operations = new ArrayList<>();
        while (iterator.hasNext()) {
            operations.addAll(reader.getVariants(iterator.next()));
        }
        assertEquals(3, operations.size());
    }

    @Test
    public void basicRead() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(3, variants.size());
    }

    private Map<String, Variant> readIntoMap() throws Exception {
        reader.open(executionContext);
        Map<String, Variant> allVariants = new HashMap<>();
        List<Variant> variants;
        while ((variants = reader.read()) != null) {
            for (Variant variant : variants) {
                allVariants.put(getStringId(variant), variant);
            }
        }
        reader.close();
        return allVariants;
    }

    private String getStringId(Variant variant) {
        return (variant.getChromosome() + "_" + variant.getStart() + "_" + variant.getReference() + "_"
                + variant.getAlternate()).toUpperCase();
    }

    @Test
    public void checkMergedInto() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(3, variants.size());


        assertTrue(variants.get(ID_1_A)
                           .getSourceEntries()
                           .stream()
                           .allMatch(e -> ID_1_MERGED_INTO.equals(e.getAttribute(MERGED_INTO_KEY))));

        assertTrue(variants.get(ID_1_T)
                           .getSourceEntries()
                           .stream()
                           .allMatch(e -> ID_1_MERGED_INTO.equals(e.getAttribute(MERGED_INTO_KEY))));

        assertTrue(variants.get(ID_2)
                           .getSourceEntries()
                           .stream()
                           .allMatch(e -> ID_2_MERGED_INTO.equals(e.getAttribute(MERGED_INTO_KEY))));
    }

    @Test
    public void checkAlleles() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(3, variants.size());

        assertEquals("G", variants.get(ID_1_A).getReference());
        assertEquals("A", variants.get(ID_1_A).getAlternate());

        assertEquals("G", variants.get(ID_1_T).getReference());
        assertEquals("T", variants.get(ID_1_T).getAlternate());

        assertEquals("T", variants.get(ID_2).getReference());
        assertEquals("G", variants.get(ID_2).getAlternate());
    }
}