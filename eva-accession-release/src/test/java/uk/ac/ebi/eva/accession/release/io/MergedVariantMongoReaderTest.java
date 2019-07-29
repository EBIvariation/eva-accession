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
import org.springframework.data.mongodb.core.MongoTemplate;
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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.CLUSTERED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.SUBMITTED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.ALLELES_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.ASSEMBLY_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.MERGED_INTO_KEY;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.SUPPORTED_BY_EVIDENCE_KEY;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpSubmittedVariantOperationEntity.json"
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

    private static final int EXPECTED_MERGED_VARIANTS = 3;

    private static final int CHUNK_SIZE = 5;

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
        reader = new MergedVariantMongoReader(ASSEMBLY, mongoClient, TEST_DB, CHUNK_SIZE);
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
        assertEquals(EXPECTED_MERGED_VARIANTS, operations.size());
    }

    @Test
    public void basicRead() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(EXPECTED_MERGED_VARIANTS, variants.size());
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
        assertEquals(EXPECTED_MERGED_VARIANTS, variants.size());

        assertTrue(variants.get(ID_1_A)
                           .getSourceEntries()
                           .stream()
                           .allMatch(e -> ID_1_MERGED_INTO.equals(e.getAttribute(MERGED_INTO_KEY))));

        assertTrue(variants.get(ID_1_T)
                           .getSourceEntries()
                           .stream()
                           .allMatch(e -> ID_1_MERGED_INTO.equals(e.getAttribute(MERGED_INTO_KEY))));
    }

    @Test
    public void checkAlleles() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(EXPECTED_MERGED_VARIANTS, variants.size());

        assertEquals("G", variants.get(ID_1_A).getReference());
        assertEquals("A", variants.get(ID_1_A).getAlternate());

        assertEquals("G", variants.get(ID_1_T).getReference());
        assertEquals("T", variants.get(ID_1_T).getAlternate());

        assertEquals("T", variants.get(ID_2).getReference());
        assertEquals("G", variants.get(ID_2).getAlternate());
    }

    @Test
    public void includeValidatedFlag() throws Exception {
        assertFlagEqualsInAllVariants(CLUSTERED_VARIANT_VALIDATED_KEY, false);
        assertFlagEqualsInAllVariants(SUBMITTED_VARIANT_VALIDATED_KEY, false);
    }

    private void assertFlagEqualsInAllVariants(String key, boolean value) throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertNotEquals(0, variants.size());
        assertTrue(variants.values().stream()
                           .flatMap(v -> v.getSourceEntries().stream())
                           .map(se -> se.getAttribute(key))
                           .map(Boolean::new)
                           .allMatch(v -> v.equals(value)));
    }

    @Test
    public void includeAssemblyMatchFlag() throws Exception {
        assertFlagEqualsInAllVariants(ASSEMBLY_MATCH_KEY, true);
    }

    @Test
    public void includeAllelesMatchFlag() throws Exception {
        assertFlagEqualsInAllVariants(ALLELES_MATCH_KEY, true);
    }

    @Test
    public void includeEvidenceFlag() throws Exception {
        assertFlagEqualsInAllVariants(SUPPORTED_BY_EVIDENCE_KEY, false);
    }

    /**
     * This test will use a different reader for assembly GCA_000001215.4 to evaluate this specific scenario:
     * - 1 Merge operation in dbsnpClusteredVariantOperationEntity (rs881301177 merged into rs80393223)
     * - 2 Submitted variants in dbsnpSubmittedVariantEntity (ss99056614, ss1986084768) with the same rs80393223
     * - 1 Update operation in dbsnpSubmittedVariantOperationEntity for ss1986084768 (Original rs881301177 was merged
     * into rs80393223)
     * - 1 Merge operation in dbsnpSubmittedVariantOperationEntity for ss1986084768
     *
     * Even though the rs80393223 was involved in a merge operation it doesn't mean all of it's associated variants were
     * also involved in that merge. Hence we should only list the variants that were updated, merge operations of
     * submitted variants should be ignored.
     */
    @Test
    public void includeOnlyMergedVariants() throws Exception {
        MergedVariantMongoReader reader2 = new MergedVariantMongoReader("GCA_000001215.4", mongoClient, TEST_DB,
                                                                        CHUNK_SIZE);
        reader2.open(executionContext);
        Map<String, Variant> allVariants = new HashMap<>();
        List<Variant> variants;
        while ((variants = reader2.read()) != null) {
            for (Variant variant : variants) {
                allVariants.put(getStringId(variant), variant);
            }
        }
        reader2.close();

        assertEquals(1, allVariants.size());
        assertEquals("rs881301177", allVariants.get("AE013599.5_7680720_T_").getMainId());
        assertEquals("rs80393223", allVariants.get("AE013599.5_7680720_T_").getSourceEntries().iterator().next()
                                              .getAttributes().get("CURR"));
    }
}
