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

import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpClass;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpSubmittedVariantEntity;

import java.sql.Date;
import java.time.LocalDateTime;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class SubSnpNoHgvsToVariantProcessorTest {

    private static final String ASSEMBLY = "AnyAssembly-1.0";

    private static final String PROJECT_ACCESSION = "HANDLE_NAME";

    private static final String BATCH_HANDLE = "HANDLE";

    private static final String BATCH_NAME = "NAME";

    private static final String CHROMOSOME = "10";

    private static final long CHROMOSOME_START = 1000000L;

    private static final String CONTIG_NAME = "Contig1";

    private static final long CONTIG_START = 20000L;

    private static final int TAXONOMY = 9999;

    private static final Date CREATED_DATE = Date.valueOf("2010-01-02");

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
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        // TODO: validated, match assembly and RS variant accession are being added into PR #28

        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(25928972L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "A",
                                                                                      "C", 14718243L, false, false,
                                                                                      false, false, 1);
        expectedVariant.setCreatedDate(LocalDateTime.parse("2004-06-23T21:45:00"));

        // TODO: compare createdDate and hash, as they are not compared in the equals method

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1984788946L, 14718243L, "T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1984788946L, "TODO_HASH",
                                                                                      ASSEMBLY, TAXONOMY,
                                                                                      PROJECT_ACCESSION, CHROMOSOME,
                                                                                      CHROMOSOME_START, "A", "G",
                                                                                      14718243L, false, false, false,
                                                                                      false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "G", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(920114L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "G",
                                                                                      "A", 14730808L, false, false,
                                                                                      false, false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1982511850L, 14730808L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "G", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1982511850L, "TODO_HASH",
                                                                                      ASSEMBLY, TAXONOMY,
                                                                                      PROJECT_ACCESSION, CHROMOSOME,
                                                                                      CHROMOSOME_START, "G", "A",
                                                                                      14730808L, false, false, false,
                                                                                      false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(181534645L, 14797051L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(181534645L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "A",
                                                                                      "G", 14797051L, false, false,
                                                                                      false, false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContigAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(823297358L, 14797051L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.FORWARD, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(823297358L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "A",
                                                                                      "G", 14797051L, false, false,
                                                                                      false, false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContigAndRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1979073615L, 10723963L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1979073615L, "TODO_HASH",
                                                                                      ASSEMBLY, TAXONOMY,
                                                                                      PROJECT_ACCESSION, CHROMOSOME,
                                                                                      CHROMOSOME_START, "G", "A",
                                                                                      10723963L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformSnpReverseContigRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(822765305L, 14510048L, "C/G", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(822765305L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "G",
                                                                                      "C", 14510048L, false, false,
                                                                                      false, false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformInsertion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "-/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "-", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1052228949L, "TODO_HASH",
                                                                                      ASSEMBLY, TAXONOMY,
                                                                                      PROJECT_ACCESSION, CHROMOSOME,
                                                                                      CHROMOSOME_START, "-", "T",
                                                                                      794529293L, false, false, false,
                                                                                      false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformDeletion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "T/-", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.DIV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(808112673L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "T",
                                                                                      "-", 794532822L, false, false,
                                                                                      false, false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformDeletionReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, "T/-", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.DIV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(820982442L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "T",
                                                                                      "-", 794525917L, false, false,
                                                                                      false, false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformVariantSupportedByEvidence() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25945162L, 14730808L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, true, "G", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(25945162L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "G",
                                                                                      "A", 14730808L, true, false,
                                                                                      false, false, 1);
        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformMultiallelicSS() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1975823489L, 13637891L, "A/C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpClass.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, CONTIG_START, false, false, "A", CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant1 = new DbsnpSubmittedVariantEntity(1975823489L, "TODO_HASH",
                                                                                       ASSEMBLY, TAXONOMY,
                                                                                       PROJECT_ACCESSION, CHROMOSOME,
                                                                                       CHROMOSOME_START, "A", "C",
                                                                                       13637891L, false, false, false,
                                                                                       false, 1);
        DbsnpSubmittedVariantEntity expectedVariant2 = new DbsnpSubmittedVariantEntity(1975823489L, "TODO_HASH",
                                                                                       ASSEMBLY, TAXONOMY,
                                                                                       PROJECT_ACCESSION, CHROMOSOME,
                                                                                       CHROMOSOME_START, "A", "G",
                                                                                       13637891L, false, false, false,
                                                                                       false, 1);
        assertTrue(variants.contains(expectedVariant1));
        assertTrue(variants.contains(expectedVariant2));
    }

    @Test
    public void leadingSlashInAllelesIsIgnored() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                                     CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "C", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "C",
                                                                                      "A", 2L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));

        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.REVERSE,
                                        Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false, false, "A",
                                        CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs);
        expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH", ASSEMBLY, TAXONOMY, PROJECT_ACCESSION,
                                                          CHROMOSOME, CHROMOSOME_START, "A", "G", 2L, false, false,
                                                          false, false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void trailingSlashInAllelesIsIgnored() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                                     CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "C", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START, "C",
                                                                                      "A", 2L, false, false, false,
                                                                                      false, 1);

        assertEquals(expectedVariant, variants.get(0));

        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "T/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.REVERSE,
                                        Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false, false, "A",
                                        CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs);
        expectedVariant = new DbsnpSubmittedVariantEntity(1L, "TODO_HASH", ASSEMBLY, TAXONOMY, PROJECT_ACCESSION,
                                                          CHROMOSOME, CHROMOSOME_START, "A", "G", 2L, false, false,
                                                          false, false, 1);

        assertEquals(expectedVariant, variants.get(0));
    }

    @Test
    public void transformShortTandemRepeat() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "(T)4/5/7", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpClass.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, CONTIG_START, false, false, "T", CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        DbsnpSubmittedVariantEntity expectedVariant1 = new DbsnpSubmittedVariantEntity(700214256L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START,
                                                                                      "T(4)", "T(5)", 439714840L, false,
                                                                                      false, false, false, 1);
        DbsnpSubmittedVariantEntity expectedVariant2 = new DbsnpSubmittedVariantEntity(700214256L, "TODO_HASH", ASSEMBLY,
                                                                                      TAXONOMY, PROJECT_ACCESSION,
                                                                                      CHROMOSOME, CHROMOSOME_START,
                                                                                      "T(4)", "T(7)", 439714840L, false,
                                                                                      false, false, false, 1);

        assertTrue(variants.contains(expectedVariant1));
        assertTrue(variants.contains(expectedVariant2));
    }

    // TODO: MNV test
    // TODO: STR test
    // TODO: variant without chromosome coordinates test
}