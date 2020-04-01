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
package uk.ac.ebi.eva.accession.dbsnp.batch.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.batch.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class})
public class DbsnpSubmittedVariantWriterTest {
    private static final int TAXONOMY_1 = 3880;

    private static final int TAXONOMY_2 = 3882;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final long EXPECTED_ACCESSION_2 = 10000000001L;

    private static final int START_1 = 100;

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = true;

    private static final Boolean MATCHES_ASSEMBLY = false;

    private static final Boolean ALLELES_MATCH = true;

    private static final Boolean VALIDATED = false;

    private static final LocalDateTime CREATED_DATE = LocalDateTime.of(2018, Month.SEPTEMBER, 18, 9, 0);

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    @Autowired
    private MongoTemplate mongoTemplate;

    private Function<ISubmittedVariant, String> hashingFunction;

    private ImportCounts importCounts;

    @Before
    public void setUp() throws Exception {
        importCounts = new ImportCounts();
        dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate, importCounts);
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
    }

    @Test
    public void saveSingleAccession() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                 "reference", "alternate", CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, CREATED_DATE);
        DbsnpSubmittedVariantEntity variant = new DbsnpSubmittedVariantEntity(EXPECTED_ACCESSION,
                                                                              hashingFunction.apply(submittedVariant),
                                                                              submittedVariant, 1);

        dbsnpSubmittedVariantWriter.write(Collections.singletonList(variant));

        List<DbsnpSubmittedVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.get(0).getAccession());
        assertEquals(1, importCounts.getSubmittedVariantsWritten());

        assertEquals(submittedVariant, accessions.get(0).getModel());
        assertEquals(CREATED_DATE, accessions.get(0).getModel().getCreatedDate());
    }

    @Test
    public void failsOnDifferentTaxonomies() throws Exception {
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig",
                                                                      START_1, "reference", "alternate",
                                                                      CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                                      MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant submittedVariant2 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig",
                                                                       START_1, "reference", "alternate",
                                                                       CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                                       MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        DbsnpSubmittedVariantEntity firstVariant = new DbsnpSubmittedVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(submittedVariant1), submittedVariant1, 1);
        DbsnpSubmittedVariantEntity secondVariant = new DbsnpSubmittedVariantEntity(
                EXPECTED_ACCESSION_2, hashingFunction.apply(submittedVariant2), submittedVariant2, 1);

        try {
            assertThrows(DuplicateKeyException.class, () ->
                    dbsnpSubmittedVariantWriter.write(Arrays.asList(firstVariant, secondVariant)));
        } finally {
            assertEquals(1, importCounts.getSubmittedVariantsWritten());
        }
    }

    @Test
    public void failsOnDuplicateVariant() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig",
                                                                 START_1, "reference", "alternate",
                                                                 CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                                 MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        DbsnpSubmittedVariantEntity variant = new DbsnpSubmittedVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(submittedVariant), submittedVariant, 1);

        try {
            assertThrows(DuplicateKeyException.class, () ->
                    dbsnpSubmittedVariantWriter.write(Arrays.asList(variant, variant)));
        } finally {
            assertEquals(1, importCounts.getSubmittedVariantsWritten());
        }
    }

}
