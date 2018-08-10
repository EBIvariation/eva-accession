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
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class})
public class DbsnpClusteredVariantWriterTest {
    private static final int TAXONOMY_1 = 3880;

    private static final int TAXONOMY_2 = 3882;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final long EXPECTED_ACCESSION_2 = 10000000001L;

    private static final int START_1 = 100;

    private static final VariantType VARIANT_TYPE = VariantType.SNV;

    private static final Boolean VALIDATED = false;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    @Autowired
    private MongoTemplate mongoTemplate;

    private Function<IClusteredVariant, String> hashingFunction;

    private ImportCounts importCounts;

    @Before
    public void setUp() throws Exception {
        importCounts = new ImportCounts();
        dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate, importCounts);
        hashingFunction = new DbsnpClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
    }

    @Test
    public void saveSingleAccession() throws Exception {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity variant = new DbsnpClusteredVariantEntity(EXPECTED_ACCESSION,
                                                                              hashingFunction.apply(clusteredVariant),
                                                                              clusteredVariant);

        dbsnpClusteredVariantWriter.write(Collections.singletonList(variant));

        List<DbsnpClusteredVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.get(0).getAccession());
        assertEquals(1, importCounts.getClusteredVariantsWritten());

        assertEquals(clusteredVariant, accessions.get(0).getModel());
    }

    @Test
    public void saveDifferentTaxonomies() throws Exception {
        ClusteredVariant firstClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                      VARIANT_TYPE, VALIDATED);
        ClusteredVariant secondClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_2, "contig", START_1,
                                                                       VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity firstVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(firstClusteredVariant), firstClusteredVariant);
        DbsnpClusteredVariantEntity secondVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION_2, hashingFunction.apply(secondClusteredVariant), secondClusteredVariant);

        dbsnpClusteredVariantWriter.write(Arrays.asList(firstVariant, secondVariant));

        List<DbsnpClusteredVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(2, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.get(0).getAccession());
        assertEquals(EXPECTED_ACCESSION_2, (long) accessions.get(1).getAccession());
        assertEquals(2, importCounts.getClusteredVariantsWritten());

        assertEquals(firstClusteredVariant, accessions.get(0).getModel());
        assertEquals(secondClusteredVariant, accessions.get(1).getModel());
    }

    @Test
    public void failsOnDuplicateVariant() throws Exception {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity variant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(clusteredVariant), clusteredVariant);

        thrown.expect(BulkOperationException.class);
        try {
            dbsnpClusteredVariantWriter.write(Arrays.asList(variant, variant));
        } finally {
            assertEquals(1, importCounts.getClusteredVariantsWritten());
        }
    }

    @Test
    public void failsOnDuplicateNonIdenticalVariant() throws Exception {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, false);
        ClusteredVariant duplicateClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, true);
        DbsnpClusteredVariantEntity variant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(clusteredVariant), clusteredVariant);
        DbsnpClusteredVariantEntity duplicateVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(duplicateClusteredVariant), duplicateClusteredVariant);

        thrown.expect(BulkOperationException.class);
        try {
            dbsnpClusteredVariantWriter.write(Arrays.asList(variant, duplicateVariant));
        } finally {
            assertEquals(1, importCounts.getClusteredVariantsWritten());
        }
    }

    @Test
    public void allNonDuplicatedRecordsWillBeWritten() {
        // batch of 6 variants, 5 unique and 1 duplicate that is in the middle of the batch
        ClusteredVariant firstClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, false);
        ClusteredVariant secondClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_2, "contig", START_1,
                                                                       VARIANT_TYPE, VALIDATED);
        ClusteredVariant thirdClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig_3", START_1,
                                                                       VARIANT_TYPE, VALIDATED);
        ClusteredVariant duplicateFirstClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                               VARIANT_TYPE, true);
        ClusteredVariant fourthClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig_4", START_1,
                                                                      VARIANT_TYPE, VALIDATED);
        ClusteredVariant fifthClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig_5", START_1,
                                                                      VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity firstVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(firstClusteredVariant), firstClusteredVariant);
        DbsnpClusteredVariantEntity secondVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION_2, hashingFunction.apply(secondClusteredVariant), secondClusteredVariant);
        DbsnpClusteredVariantEntity thirdVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION_2 + 1, hashingFunction.apply(thirdClusteredVariant), thirdClusteredVariant);
        DbsnpClusteredVariantEntity fourthVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION_2 + 2, hashingFunction.apply(fourthClusteredVariant), fourthClusteredVariant);
        DbsnpClusteredVariantEntity duplicateVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(duplicateFirstClusteredVariant), duplicateFirstClusteredVariant);
        DbsnpClusteredVariantEntity fifthVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION_2 + 3, hashingFunction.apply(fifthClusteredVariant), fifthClusteredVariant);
        List<DbsnpClusteredVariantEntity> batch = Arrays.asList(firstVariant, secondVariant, thirdVariant,
                                                                duplicateVariant, fourthVariant, fifthVariant);

        // we expect an exception caused by the duplicate variant when inserting the batch
        boolean exceptionThrown = false;
        thrown.expect(BulkOperationException.class);
        try {
            dbsnpClusteredVariantWriter.write(
                    batch);
        } finally {
            // 5 variants should have been inserted, and the duplicate one is not any of them
            List<DbsnpClusteredVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                              DbsnpClusteredVariantEntity.class);
            assertEquals(5, accessions.size());
            assertFalse(accessions.contains(duplicateVariant));
            assertEquals(5, importCounts.getClusteredVariantsWritten());
        }
    }
}
