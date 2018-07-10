/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.accession.dbsnp.jobs.steps.processors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpSubmittedVariantEntity;

import java.sql.Date;
import java.time.LocalDateTime;

import static org.junit.Assert.assertEquals;

public class SubSnpNoHgvsToVariantProcessorTest {

    public static final String CHICKEN_ASSEMBLY_5 = "Gallus_gallus-5.0";

    SubSnpNoHgvsToVariantProcessor processor;

    @Before
    public void setUp() throws Exception {
        processor = new SubSnpNoHgvsToVariantProcessor();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void transformSnp() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(3035489735L, 1135402630L, "C/T", CHICKEN_ASSEMBLY_5,
                                                     "CHICKEN_SDAU", "JININGBAIRI UNINFECTED", "25", 920114L,
                                                     "NT_456018", Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, 49575L, false, false, "C",
                                                     Date.valueOf("2017-08-22"), 9031);

        DbsnpSubmittedVariantEntity variant = processor.process(subSnpNoHgvs);
        // TODO: supported by evidence?
        // TODO: validated, match assembly and RS variant accession are being added into PR #28

        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(920114L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "CHICKEN_SDAU_JININGBAIRI " +
                                                                                              "UNINFECTED",
                                                                                      "25", 920114, "C", "T",
                                                                                      1135402630L, false, false, false,
                                                                                      false, 1);
        expectedVariant.setCreatedDate(LocalDateTime.parse("2017-08-22T13:22:00"));
        // TODO: is comparing well the variants? (the date is not the same)
        assertEquals(expectedVariant, variant);

    }

    @Test
    public void transformReverseRsSnp() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "C/T", CHICKEN_ASSEMBLY_5, "CFG-UPPSALA",
                                                     "CHICK_WGS_RESEQ_PAPER_CHR32", "11", 11857590L, "NT_455934",
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     375024L, false, false, "G", Date.valueOf("2010-02-03"), 9031);

        DbsnpSubmittedVariantEntity variant = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(920114L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "CFG-UPPSALA_CHICK_WGS_RESEQ_PAPER_CHR32",
                                                                                      "11", 11857590, "G", "A",
                                                                                      14730808L, false, false, false,
                                                                                      false, 1);
        expectedVariant.setCreatedDate(LocalDateTime.parse("2017-08-22T13:22:00"));

        // TODO: this test fails because the alleles are not being complemented in SubSnpNoHgvs class
        assertEquals(expectedVariant, variant);

    }
}