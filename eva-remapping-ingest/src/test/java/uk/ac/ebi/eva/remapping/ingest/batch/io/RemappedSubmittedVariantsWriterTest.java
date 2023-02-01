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
 *
 */
package uk.ac.ebi.eva.remapping.ingest.batch.io;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.remapping.ingest.batch.listeners.RemappingIngestCounts;
import uk.ac.ebi.eva.remapping.ingest.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.remapping.ingest.test.rule.FixSpringMongoDbRule;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames.REMAPPED_SUBMITTED_VARIANTS_WRITER;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:ingest-remapped-variants.properties")
public class RemappedSubmittedVariantsWriterTest {

    private static final String TEST_DB = "test-ingest-remapping";

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(REMAPPED_SUBMITTED_VARIANTS_WRITER)
    private RemappedSubmittedVariantsWriter writer;

    private final Function<ISubmittedVariant, String> hashingFunction =
            new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Autowired
    private RemappingIngestCounts remappingIngestCounts;

    @After
    public void tearDown() {
        mongoClient.dropDatabase(TEST_DB);
        remappingIngestCounts.resetCounts();
    }

    private SubmittedVariantEntity createSve(Long accession, long start, String ref, String alt,
                                             LocalDateTime createdDate, String remappedFrom) {
        SubmittedVariant model = new SubmittedVariant("GCA_000000001.2", 1000, "projectId_1", "CM000002.1",
                                                      start, ref, alt, 3000000002L);
        String hash = hashingFunction.apply(model);
        SubmittedVariantEntity sve = new SubmittedVariantEntity(accession, hash, model, 1);
        sve.setCreatedDate(createdDate);
        sve.setRemappedFrom(remappedFrom);
        return sve;
    }

    private SubmittedVariantEntity createSveInAssembly(String assembly,
                                                       Long accession, long start, String ref, String alt,
                                                       LocalDateTime createdDate, String remappedFrom) {
        SubmittedVariant model = new SubmittedVariant(assembly, 1000, "projectId_1", "CM000002.1",
                                                      start, ref, alt, 3000000002L);
        String hash = hashingFunction.apply(model);
        SubmittedVariantEntity sve = new SubmittedVariantEntity(accession, hash, model, 1);
        sve.setCreatedDate(createdDate);
        sve.setRemappedFrom(remappedFrom);
        return sve;
    }

    private void assertRemappingIngestCounts(int ingested, int skipped, int discarded) {
        assertEquals(ingested, remappingIngestCounts.getRemappedVariantsIngested());
        assertEquals(skipped, remappingIngestCounts.getRemappedVariantsSkipped());
        assertEquals(discarded, remappingIngestCounts.getRemappedVariantsDiscarded());
    }

    private void assertDatabaseCounts(int newSves) {
        assertEquals(newSves, mongoTemplate.findAll(SubmittedVariantEntity.class).size());
    }

    @Test
    public void testDoesNotWriteNewDuplicateAccessions_discardFromInput() {
        List<SubmittedVariantEntity> svesInDb = Arrays.asList(
                // Oldest source (non-remapped) SVE
                createSveInAssembly("GCA_000000001.1", 5000000002L, 1000, "C", "T", LocalDateTime.now(), null),
                // SVE remapped from oldest SVE
                createSve(5000000002L, 2000, "C", "T", LocalDateTime.now(), "GCA_000000001.1"),
                // Another (newer) non-remapped SVE
                createSveInAssembly("GCA_000000001.5", 5000000002L, 2100, "C", "T", LocalDateTime.now(), null));

        mongoTemplate.insert(svesInDb, SubmittedVariantEntity.class);

        List<SubmittedVariantEntity> svesToWrite = Collections.singletonList(
                // SVE remapped from newer source SVE should be discarded
                createSve(5000000002L, 2100, "C", "T", LocalDateTime.now(), "GCA_000000001.5"));
        writer.write(svesToWrite);
        assertRemappingIngestCounts(0, 0, 1);
        assertDatabaseCounts(3);
    }

    @Test
    public void testDoesNotWriteNewDuplicateAccessions_discardFromDatabase() {
        // Same SS but remapped is already in the db
        SubmittedVariantEntity remappedSve = createSve(5000000004L, 1000, "C", "T", LocalDateTime.now(), "GCA_000000001.1");
        mongoTemplate.insert(remappedSve);

        List<SubmittedVariantEntity> svesToWrite = Collections.singletonList(
                createSve(5000000004L, 1100, "C", "T", LocalDateTime.now(), null));
        writer.write(svesToWrite);
        assertRemappingIngestCounts(1, 0, 1);
        assertDatabaseCounts(1);
    }

    @Test
    public void testDeduplicatesAccessionsFromInput() {
        List<SubmittedVariantEntity> svesToWrite = Arrays.asList(
                createSve(5000000003L, 1100, "C", "T", LocalDateTime.now(), "GCA_000000001.1"),
                createSve(5000000003L, 1100, "A", "T", LocalDateTime.now(), "GCA_000000001.1"));
        writer.write(svesToWrite);
        assertRemappingIngestCounts(1, 0, 1);
        assertDatabaseCounts(1);
    }

    @Test
    public void testDiscardsDuplicateHashes() {
        SubmittedVariantEntity sveWithSameHash = createSve(5000000004L, 1100, "C", "T", LocalDateTime.now(), null);
        mongoTemplate.insert(sveWithSameHash);

        List<SubmittedVariantEntity> svesToWrite = Collections.singletonList(
                createSve(5000000005L, 1100, "C", "T", LocalDateTime.now(), "GCA_000000001.1"));
        writer.write(svesToWrite);
        assertRemappingIngestCounts(0, 0, 1);
        assertDatabaseCounts(1);
    }

    @Test
    public void testDeduplicatesHashesFromInput() {
        SubmittedVariantEntity duplicateSve = createSve(5000000003L, 1100, "C", "T", LocalDateTime.now(), "GCA_000000001.1");
        mongoTemplate.insert(duplicateSve);

        List<SubmittedVariantEntity> svesToWrite = Arrays.asList(
                // Exact duplicate of what's in the db already => skipped
                createSve(5000000003L, 1100, "C", "T", LocalDateTime.now(), "GCA_000000001.1"),
                // Duplicate hash but different accession => discard operation
                createSve(5000000004L, 1100, "C", "T", LocalDateTime.now(), "GCA_000000001.1"));
        writer.write(svesToWrite);
        assertRemappingIngestCounts(0, 1, 1);
        assertDatabaseCounts(1);
    }

    @Test
    public void testDuplicateHashAndAccessionButDifferentSVEs() {
        SubmittedVariantEntity sve = createSve(5000000003L, 1100, "C", "T", LocalDateTime.now(), "GCA_000000001.1");
        mongoTemplate.insert(sve);

        // Same hash and accession as what's in db, but different in some other attribute
        SubmittedVariantEntity duplicateSve = createSve(5000000003L, 1100, "C", "T", LocalDateTime.now(), "GCA_000000001.1");
        duplicateSve.setBackPropagatedVariantAccession(12345L);
        List<SubmittedVariantEntity> svesToWrite = Collections.singletonList(duplicateSve);
        writer.write(svesToWrite);
        assertRemappingIngestCounts(0, 0, 1);
        assertDatabaseCounts(1);
    }

    @Test
    public void testIdempotentWrites() {
        mongoTemplate.insert(createSve(5000000002L, 1000, "C", "T", LocalDateTime.now(), "GCA_000000001.1"));

        List<SubmittedVariantEntity> svesToWrite = Collections.singletonList(
                createSve(5000000002L, 1100, "C", "T", LocalDateTime.now(), null));
        writer.write(svesToWrite);
        assertRemappingIngestCounts(1, 0, 1);

        Set<SubmittedVariantEntity> sveAfterFirstWrite = new HashSet<>(
                mongoTemplate.findAll(SubmittedVariantEntity.class));
        Set<SubmittedVariantOperationEntity> svoeAfterFirstWrite = new HashSet<>(
                mongoTemplate.findAll(SubmittedVariantOperationEntity.class));

        remappingIngestCounts.resetCounts();
        writer.write(svesToWrite);
        assertRemappingIngestCounts(0, 1, 0);

        Set<SubmittedVariantEntity> sveAfterSecondWrite = new HashSet<>(
                mongoTemplate.findAll(SubmittedVariantEntity.class));
        Set<SubmittedVariantOperationEntity> svoeAfterSecondWrite = new HashSet<>(
                mongoTemplate.findAll(SubmittedVariantOperationEntity.class));

        assertEquals(sveAfterFirstWrite, sveAfterSecondWrite);
        assertEquals(svoeAfterFirstWrite, svoeAfterSecondWrite);
    }

    @Test
    public void testDuplicateAccessionInDb_discardFromDb() {
        // Scenario assumes duplicate SS on one assembly already present in database
        LocalDateTime now = LocalDateTime.now();
        List<SubmittedVariantEntity> svesInDb = Arrays.asList(
                createSve(5000000003L, 1100, "C", "T", now, "GCA_000000001.1"),
                createSve(5000000003L, 1200, "C", "T", now, "GCA_000000001.1")
        );
        mongoTemplate.insert(svesInDb, SubmittedVariantEntity.class);
        // Use the exact createdDate present in the database
        now = mongoTemplate.findAll(SubmittedVariantEntity.class).get(0).getCreatedDate();

        // If we ingest another variant with the same SS, the writer will discard duplicates from the database as well.
        List<SubmittedVariantEntity> firstBatch = Collections.singletonList(
                createSve(5000000003L, 1400, "C", "T", now, "GCA_000000001.1")
        );
        writer.write(firstBatch);
        assertRemappingIngestCounts(0, 0, 2);
        assertDatabaseCounts(1);

        List<SubmittedVariantEntity> secondBatch = Arrays.asList(
                createSve(5000000003L, 1400, "C", "T", now, "GCA_000000001.1"),
                createSve(5000000004L, 1400, "C", "T", now, "GCA_000000001.1")
        );
        writer.write(secondBatch);
        assertRemappingIngestCounts(0, 1, 3);
        assertDatabaseCounts(1);
    }

    @Test
    public void testDuplicateAccessionInDb_skipFromInput() {
        // Scenario assumes duplicate SS on one assembly already present in database
        LocalDateTime now = LocalDateTime.now();
        List<SubmittedVariantEntity> svesInDb = Arrays.asList(
                createSve(5000000003L, 1100, "C", "T", now, "GCA_000000001.1"),
                createSve(5000000003L, 1200, "C", "T", now, "GCA_000000001.1")
        );
        mongoTemplate.insert(svesInDb, SubmittedVariantEntity.class);
        // Use the exact createdDate present in the database
        now = mongoTemplate.findAll(SubmittedVariantEntity.class).get(0).getCreatedDate();

        // This case does NOT deduplicate the accessions already present in db, as the duplicate SS in the batch is
        // filtered out based on duplicate hash before we query the db for that accessions
        List<SubmittedVariantEntity> svesToWrite = Arrays.asList(
                createSve(5000000003L, 1400, "C", "T", now, "GCA_000000001.1"),
                createSve(5000000003L, 1400, "C", "T", now, "GCA_000000001.1"),
                createSve(5000000004L, 1400, "C", "T", now, "GCA_000000001.1")
        );
        writer.write(svesToWrite);
        assertRemappingIngestCounts(1, 2, 0);
        assertDatabaseCounts(3);
    }

}
