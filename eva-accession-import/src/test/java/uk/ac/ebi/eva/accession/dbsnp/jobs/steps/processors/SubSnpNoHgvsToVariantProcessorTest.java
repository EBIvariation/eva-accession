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
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class SubSnpNoHgvsToVariantProcessorTest {

    public static final String CHICKEN_ASSEMBLY_5 = "Gallus_gallus-5.0";

    private static final String CONTIG_NAME = "Contig1";

    SubSnpNoHgvsToVariantProcessor processor;

    @Before
    public void setUp() throws Exception {
        processor = new SubSnpNoHgvsToVariantProcessor();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void transformSnpForwardOrientations() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A/C", CHICKEN_ASSEMBLY_5, "BGI",
                                                     "CHICKEN_SNPS_BROILER", "W", 880493L, "NT_456074",
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     339900L, false, false, "A", Date.valueOf("2004-06-23"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        // TODO: validated, match assembly and RS variant accession are being added into PR #28

        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(25928972L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "BGI_CHICKEN_SNPS_BROILER", "W",
                                                                                      880493L, "A", "C", 14718243L,
                                                                                      false, false, false, false, 1);
        expectedVariant.setCreatedDate(LocalDateTime.parse("2004-06-23T21:45:00"));

        // TODO: compare createdDate and hash, as they are not compared in the equals method

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1984788946L, 14718243L, "T/C", CHICKEN_ASSEMBLY_5, "LBA_ESALQ",
                                                     "SNP_28TTCC_CB", "W", 880493L, "NT_456074", Orientation.REVERSE,
                                                     Orientation.FORWARD, Orientation.FORWARD, 339900L, false, false,
                                                     "A", Date.valueOf("2016-03-19"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1984788946L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "LBA_ESALQ_SNP_28TTCC_CB",
                                                                                      "W", 880493L, "A", "G",
                                                                                      14718243L, false, false, false,
                                                                                      false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "C/T", CHICKEN_ASSEMBLY_5, "CFG-UPPSALA",
                                                     "CHICK_WGS_RESEQ_PAPER_CHR32", "11", 11857590L, "NT_455934",
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     375024L, false, false, "G", Date.valueOf("2010-02-03"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(920114L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "CFG-UPPSALA_CHICK_WGS_RESEQ_PAPER_CHR32",
                                                                                      "11", 11857590, "G", "A",
                                                                                      14730808L, false, false, false,
                                                                                      false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1982511850L, 14730808L, "G/A", CHICKEN_ASSEMBLY_5, "LBA_ESALQ",
                                                     "SNP_28TTCC_CB", "11", 11857590L, "NT_455934", Orientation.REVERSE,
                                                     Orientation.REVERSE, Orientation.FORWARD, 375024L, false, false,
                                                     "G", Date.valueOf("2016-03-19"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1982511850L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "LBA_ESALQ_SNP_28TTCC_CB",
                                                                                      "11", 11857590, "G", "A",
                                                                                      14730808L, false, false, false,
                                                                                      false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(181534645L, 14797051L, "C/T", CHICKEN_ASSEMBLY_5, "CBCB",
                                                     "DUPLICATION MIS-ASSEMBLY 2009 - CHICKEN", "1", 9869060L,
                                                     "NT_455705", Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.REVERSE, 540970L, false, false, "T",
                                                     Date.valueOf("2009-12-07"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(181534645L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "CBCB_DUPLICATION MIS-ASSEMBLY 2009 - CHICKEN",
                                                                                      "1", 9869060L, "A", "G",
                                                                                      14797051L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContigAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(823297358L, 14797051L, "G/A", CHICKEN_ASSEMBLY_5, "CNU_JH_AMG",
                                                     "KOREAN_CHICKEN_Y24", "1", 9869060L, "NT_455705",
                                                     Orientation.REVERSE, Orientation.FORWARD, Orientation.REVERSE,
                                                     540970L, false, false, "T", Date.valueOf("2013-06-18"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(823297358L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "CNU_JH_AMG_KOREAN_CHICKEN_Y24",
                                                                                      "1", 9869060L, "A", "G",
                                                                                      14797051L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContigAndRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1979073615L, 10723963L, "G/A", CHICKEN_ASSEMBLY_5, "LBA_ESALQ",
                                                     "SNP_28TTCC_CB", "4", 65914909L, "NT_455856", Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, 3825984L, false, false,
                                                     "C", Date.valueOf("2016-03-18"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1979073615L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "LBA_ESALQ_SNP_28TTCC_CB",
                                                                                      "4", 65914909L, "G", "A",
                                                                                      10723963L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContigRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(822765305L, 14510048L, "C/G", CHICKEN_ASSEMBLY_5, "CNU_JH_AMG",
                                                     "KOREAN_CHICKEN_Y24", "5", 432354L, "NT_455869",
                                                     Orientation.REVERSE, Orientation.REVERSE, Orientation.REVERSE,
                                                     5268917L, false, false, "C", Date.valueOf("2013-06-18"),
                                                     9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(822765305L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "CNU_JH_AMG_KOREAN_CHICKEN_Y24",
                                                                                      "5", 432354L, "G", "C",
                                                                                      14510048L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test public void transformInsertion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "-/T", CHICKEN_ASSEMBLY_5, "HANDLE",
                                                     "NAME", "1", 69527468L, "NT_455715", Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, 2805800L, false, false,
                                                     "-", Date.valueOf("2010-01-01"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1052228949L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "HANDLE_NAME", "1", 69527468L,
                                                                                      "-", "T", 794529293L, false,
                                                                                      false, false, false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test public void transformDeletion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "T/-", CHICKEN_ASSEMBLY_5, "HANDLE",
                                                     "NAME", "2", 90014736L, "NT_455791", Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, 3578926L, false, false,
                                                     "T", Date.valueOf("2010-01-01"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(808112673L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "HANDLE_NAME", "2", 90014736L,
                                                                                      "T", "-", 794532822L, false,
                                                                                      false, false, false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test public void transformDeletionReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, "T/-", CHICKEN_ASSEMBLY_5, "HANDLE",
                                                     "NAME", "5", 10000L, CONTIG_NAME, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, 2000L, false, false, "A",
                                                     Date.valueOf("2010-01-01"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(820982442L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "HANDLE_NAME", "5", 10000L,
                                                                                      "T", "-", 794525917L, false,
                                                                                      false, false, false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformVariantSupportedByEvidence() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25945162L, 14730808L, "C/T", CHICKEN_ASSEMBLY_5, "BGI",
                                                     "CHICKEN_SNPS_BROILER", "11", 11857590L, "NT_455934",
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     375024L, false, true, "G", Date.valueOf("2004-06-23"),
                                                     9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(25945162L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "BGI_CHICKEN_SNPS_BROILER",
                                                                                      "11", 11857590L, "G", "A",
                                                                                      14730808L, true, false, false,
                                                                                      false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformMultiallelicSS() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1975823489L, 13637891L, "A/C/G", CHICKEN_ASSEMBLY_5, "LBA_ESALQ",
                                                     "SNP_28TTCC_CB", "2", 88762120L, "NT_455791", Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, 2326310L, false, false,
                                                     "A", Date.valueOf("2016-03-18"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant1 = new DbsnpSubmittedVariantEntity(1975823489L, "TODO_HASH",
                                                                                       CHICKEN_ASSEMBLY_5, 9031,
                                                                                       "LBA_ESALQ_SNP_28TTCC_CB", "2",
                                                                                       88762120L, "A", "C", 13637891L,
                                                                                       false, false, false, false, 1);
        DbsnpSubmittedVariantEntity expectedVariant2 = new DbsnpSubmittedVariantEntity(1975823489L, "TODO_HASH",
                                                                                       CHICKEN_ASSEMBLY_5, 9031,
                                                                                       "LBA_ESALQ_SNP_28TTCC_CB", "2",
                                                                                       88762120L, "A", "G", 13637891L,
                                                                                       false, false, false, false, 1);
        assertTrue(variants.contains(expectedVariant1));
        assertTrue(variants.contains(expectedVariant2));
    }

    @Test
    public void leadingSlashInAllelesIsIgnored() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/A/C", CHICKEN_ASSEMBLY_5, "BATCH", "HANDLE", "1", 1000L,
                                                     "CONTIG", Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, 100L, false, false, "C",
                                                     Date.valueOf("2003-02-03"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "BATCH_HANDLE", "1", 1000L, "C",
                                                                                      "A", 2L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));

        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/T/C", CHICKEN_ASSEMBLY_5, "BATCH", "HANDLE", "1", 1000L, "CONTIG",
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, 100L, false,
                                        false, "A", Date.valueOf("2003-02-03"), 9031);

        variants = processor.process(subSnpNoHgvs);
        expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH", CHICKEN_ASSEMBLY_5, 9031, "BATCH_HANDLE",
                                                          "1", 1000L, "A", "G", 2L, false, false, false, false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void trailingSlashInAllelesIsIgnored() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A/C/", CHICKEN_ASSEMBLY_5, "BATCH", "HANDLE", "1", 1000L,
                                                     "CONTIG", Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, 100L, false, false, "C",
                                                     Date.valueOf("2003-02-03"), 9031);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH",
                                                                                      CHICKEN_ASSEMBLY_5, 9031,
                                                                                      "BATCH_HANDLE", "1", 1000L, "C",
                                                                                      "A", 2L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));

        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "T/C/", CHICKEN_ASSEMBLY_5, "BATCH", "HANDLE", "1", 1000L, "CONTIG",
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, 100L, false,
                                        false, "A", Date.valueOf("2003-02-03"), 9031);

        variants = processor.process(subSnpNoHgvs);
        expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH", CHICKEN_ASSEMBLY_5, 9031, "BATCH_HANDLE",
                                                          "1", 1000L, "A", "G", 2L, false, false, false, false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }
}