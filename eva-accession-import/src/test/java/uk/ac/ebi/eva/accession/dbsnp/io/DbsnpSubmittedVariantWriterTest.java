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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.test.MongoTestConfiguration;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class DbsnpSubmittedVariantWriterTest {
    private static final int TAXONOMY_1 = 3880;

    private static final int TAXONOMY_2 = 3882;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final long EXPECTED_ACCESSION_2 = 10000000001L;

    private static final String CONTIG_1 = "contig_1";

    private static final String CONTIG_2 = "contig_2";

    private static final int START_1 = 100;

    private static final int START_2 = 200;

    private static final String ALTERNATE_ALLELE = "T";

    private static final String REFERENCE_ALLELE = "A";

    private static final int ACCESSION_COLUMN = 2;

    private static final String ACCESSION_PREFIX = "ss";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = null;

    private static final Boolean MATCHES_ASSEMBLY = null;

    private static final Boolean ALLELES_MATCH = null;

    private static final Boolean VALIDATED = null;

    private DbsnpSubmittedVariantWriter dbsnpSubmittedVariantWriter;

    @Autowired
    private MongoTemplate mongoTemplate;

    private Function<ISubmittedVariant, String> hashingFunction;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        dbsnpSubmittedVariantWriter = new DbsnpSubmittedVariantWriter(mongoTemplate);
        hashingFunction = new DbsnpSubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
    }

    @Test
    public void saveSingleAccession() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                 "reference", "alternate",
                                                                 CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                                 MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED);
        DbsnpSubmittedVariantEntity variant = new DbsnpSubmittedVariantEntity(EXPECTED_ACCESSION,
                                                                              hashingFunction.apply(submittedVariant),
                                                                              submittedVariant);

        dbsnpSubmittedVariantWriter.write(Collections.singletonList(variant));

        List<DbsnpSubmittedVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.get(0).getAccession());

        assertEquals(submittedVariant, new SubmittedVariant(accessions.get(0)));
    }

    @Test
    public void saveDifferentTaxonomies() throws Exception {
        SubmittedVariant firstSubmittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig",
                                                                      START_1, "reference", "alternate",
                                                                      CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                                      MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED);
        SubmittedVariant secondSubmittedVariant = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig",
                                                                       START_1, "reference", "alternate",
                                                                       CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                                       MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED);
        DbsnpSubmittedVariantEntity firstVariant = new DbsnpSubmittedVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(firstSubmittedVariant), firstSubmittedVariant);
        DbsnpSubmittedVariantEntity secondVariant = new DbsnpSubmittedVariantEntity(
                EXPECTED_ACCESSION_2, hashingFunction.apply(secondSubmittedVariant), secondSubmittedVariant);

        dbsnpSubmittedVariantWriter.write(Arrays.asList(firstVariant, secondVariant));

        List<DbsnpSubmittedVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                          DbsnpSubmittedVariantEntity.class);
        assertEquals(2, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.get(0).getAccession());
        assertEquals(EXPECTED_ACCESSION_2, (long) accessions.get(1).getAccession());

        assertEquals(firstSubmittedVariant, new SubmittedVariant(accessions.get(0)));
        assertEquals(secondSubmittedVariant, new SubmittedVariant(accessions.get(1)));
    }

    @Test
    public void failsOnDuplicateVariant() throws Exception {
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig",
                                                                 START_1, "reference", "alternate",
                                                                 CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                                 MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED);
        DbsnpSubmittedVariantEntity variant = new DbsnpSubmittedVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(submittedVariant), submittedVariant);

        thrown.expect(RuntimeException.class);
        dbsnpSubmittedVariantWriter.write(Arrays.asList(variant, variant));
    }

}
