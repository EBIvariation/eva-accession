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
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.ALLELES_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.ASSEMBLY_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.STUDY_ID_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.SUBMITTED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.SUPPORTED_BY_EVIDENCE_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.VARIANT_CLASS_KEY;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class AccessionedVariantMongoReaderTest {

    private static final String ASSEMBLY_ACCESSION_1 = "GCF_000409795.2";

    private static final String ASSEMBLY_ACCESSION_2 = "GCF_000001735.3";

    private static final String ASSEMBLY_ACCESSION_3 = "GCF_000372685.1";

    private static final String ASSEMBLY_ACCESSION_4 = "GCF_000309985.1";

    private static final String ASSEMBLY_ACCESSION_5 = "GCF_000003055.6";

    private static final String TEST_DB = "test-db";

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY = "dbsnpClusteredVariantEntity";

    private static final String RS_1 = "rs869808637";

    private static final String RS_2 = "rs869927931";

    private static final String RS_3 = "rs347048227";

    private static final String RS_4 = "rs109798407";

    private static final String RS_5 = "rs109920405";

    private static final String RS_1_G_A = "CM001954.1_5_G_A";

    private static final String RS_1_G_T = "CM001954.1_5_G_T";

    private static final String RS_2_T_G = "CM001941.2_13_T_G";

    private static final String RS_3_G_A = "CP002685.1_4758626_G_A";

    private static final String RS_3_G_T = "CP002685.1_4758626_G_T";

    private static final String RS_3_G_C = "CP002685.1_4758626_G_C";

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
        MongoDatabase db = mongoClient.getDatabase(TEST_DB);
        MongoCollection<Document> collection = db.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY);

        AggregateIterable<Document> result = collection.aggregate(reader.buildAggregation())
                                                       .allowDiskUse(true)
                                                       .useCursor(true);

        MongoCursor<Document> cursor = result.iterator();

        List<Variant> variants = new ArrayList<>();
        while (cursor.hasNext()) {
            Document clusteredVariant = cursor.next();
            variants.addAll(reader.getVariants(clusteredVariant));
        }
        assertEquals(3, variants.size());
     }

    @Test
    public void reader() throws Exception {
        List<Variant> variants = readIntoList();
        assertEquals(3, variants.size());
    }

    private List<Variant> readIntoList() throws Exception {
        reader.open(executionContext);
        List<Variant> allVariants = new ArrayList<>();
        List<Variant> variants;
        while ((variants = reader.read()) != null) {
            allVariants.addAll(variants);
        }
        reader.close();
        return allVariants;
    }

    @Test
    public void linkedSubmittedVariants() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(3, variants.size());
        assertEquals(2, variants.values().stream().filter(v -> v.getMainId().equals(RS_1)).count());
        assertEquals(1, variants.values().stream().filter(v -> v.getMainId().equals(RS_2)).count());
        assertEquals(1, variants.get(RS_1_G_A).getSourceEntries().size());
        assertEquals(2, variants.get(RS_1_G_T).getSourceEntries().size());
        assertEquals(1, variants.get(RS_2_T_G).getSourceEntries().size());
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
    public void queryOtherAssembly() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_2, mongoClient, TEST_DB);
        Map<String, Variant> variants = readIntoMap();

        assertEquals(3, variants.size());
        assertEquals(3, variants.values().stream().filter(v -> v.getMainId().equals(RS_3)).count());
        assertEquals(2, variants.get(RS_3_G_A).getSourceEntries().size());
        assertEquals(2, variants.get(RS_3_G_T).getSourceEntries().size());
        assertEquals(1, variants.get(RS_3_G_C).getSourceEntries().size());
    }

    @Test
    public void snpVariantClassAttribute() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(3, variants.size());
        String snpSequenceOntology = "SO:0001483";
        assertTrue(variants
                           .get(RS_1_G_A)
                           .getSourceEntries()
                           .stream()
                           .allMatch(se -> snpSequenceOntology.equals(se.getAttribute(VARIANT_CLASS_KEY))));
        assertTrue(variants
                           .get(RS_1_G_T)
                           .getSourceEntries()
                           .stream()
                           .allMatch(se -> snpSequenceOntology.equals(se.getAttribute(VARIANT_CLASS_KEY))));
        assertTrue(variants
                           .get(RS_2_T_G)
                           .getSourceEntries()
                           .stream()
                           .allMatch(se -> snpSequenceOntology.equals(se.getAttribute(VARIANT_CLASS_KEY))));
    }

    @Test
    public void insertionVariantClassAttribute() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_4, mongoClient, TEST_DB);
        List<Variant> variants = readIntoList();
        assertEquals(1, variants.size());
        String insertionSequenceOntology = "SO:0000667";
        assertTrue(variants.get(0)
                           .getSourceEntries()
                           .stream()
                           .allMatch(se -> insertionSequenceOntology.equals(se.getAttribute(VARIANT_CLASS_KEY))));
    }

    @Test
    public void otherVariantClasses() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_5, mongoClient, TEST_DB);
        List<Variant> variants = readIntoList();
        assertEquals(4, variants.size());
        String indelSequenceOntology = "SO:1000032";
        String tandemRepeatSequenceOntology = "SO:0000705";
        assertEquals(3, variants.stream()
                                .flatMap(v -> v.getSourceEntries().stream())
                                .filter(se -> tandemRepeatSequenceOntology.equals(se.getAttribute(VARIANT_CLASS_KEY)))
                                .count());
        assertEquals(1, variants.stream()
                                .flatMap(v -> v.getSourceEntries().stream())
                                .filter(se -> indelSequenceOntology.equals(se.getAttribute(VARIANT_CLASS_KEY)))
                                .count());
    }

    @Test
    public void studyIdAttribute() throws Exception {
        Map<String, Variant> variants = readIntoMap();
        assertEquals(3, variants.size());

        String studyId;
        studyId = "PRJEB7923";
        assertEquals(studyId, variants.get(RS_1_G_A).getSourceEntry(studyId, studyId).getAttribute(STUDY_ID_KEY));
        studyId = "PRJEB9999";
        assertEquals(studyId, variants.get(RS_1_G_T).getSourceEntry(studyId, studyId).getAttribute(STUDY_ID_KEY));
        studyId = "PRJEB7923";
        assertEquals(studyId, variants.get(RS_2_T_G).getSourceEntry(studyId, studyId).getAttribute(STUDY_ID_KEY));
    }

    @Test
    public void clusteredVariantWithoutSubmittedVariants() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_3, mongoClient, TEST_DB);
        List<Variant> variants = readIntoList();
        assertEquals(0, variants.size());
    }

    @Test
    public void includeValidatedFlag() throws Exception {
        assertFlagEqualsInAllVariants(VALIDATED_KEY, false);
        assertFlagEqualsInAllVariants(SUBMITTED_VARIANT_VALIDATED_KEY, false);
    }

    private void assertFlagEqualsInAllVariants(String key, boolean value) throws Exception {
        List<Variant> variants = readIntoList();
        assertNotEquals(0, variants.size());
        assertTrue(variants.stream()
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
        assertFlagEqualsInAllVariants(SUPPORTED_BY_EVIDENCE_KEY, true);
    }

    @Test
    public void includeValidatedNonDefaultFlag() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_5, mongoClient, TEST_DB);
        assertFlagEqualsInAllVariants(SUBMITTED_VARIANT_VALIDATED_KEY, true);
        assertFlagEqualsInRS(VALIDATED_KEY, false, RS_4);
        assertFlagEqualsInRS(VALIDATED_KEY, true, RS_5);
    }

    private void assertFlagEqualsInRS(String key, boolean value, String clusteredVariantAccession) throws Exception {
        List<Variant> variants = readIntoList();
        assertNotEquals(0, variants.size());
        assertTrue(variants.stream()
                           .filter(v -> v.getMainId().equals(clusteredVariantAccession))
                           .flatMap(v -> v.getSourceEntries().stream())
                           .map(se -> se.getAttribute(key))
                           .map(Boolean::new)
                           .allMatch(v -> v.equals(value)));
    }


    @Test
    public void includeAssemblyMatchNonDefaultFlag() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_4, mongoClient, TEST_DB);
        assertFlagEqualsInAllVariants(ASSEMBLY_MATCH_KEY, false);
    }

    @Test
    public void includeAllelesMatchNonDefaultFlag() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_4, mongoClient, TEST_DB);
        assertFlagEqualsInAllVariants(ALLELES_MATCH_KEY, false);
    }

    @Test
    public void includeEvidenceNonDefaultFlag() throws Exception {
        reader = new AccessionedVariantMongoReader(ASSEMBLY_ACCESSION_5, mongoClient, TEST_DB);
        assertFlagEqualsInAllVariants(SUPPORTED_BY_EVIDENCE_KEY, false);
    }
}
