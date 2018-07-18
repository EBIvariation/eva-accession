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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpClass;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SubSnpNoHgvsToClusteredVariantProcessorTest {

    private static final String ASSEMBLY = "AnyAssembly-1.0";

    private static final String PROJECT_ACCESSION = "HANDLE_NAME";

    private static final String BATCH_HANDLE = "HANDLE";

    private static final String BATCH_NAME = "NAME";

    private static final String CHROMOSOME = "10";

    private static final long CHROMOSOME_START = 1000000L;

    private static final String CONTIG_NAME = "Contig1";

    private static final long CONTIG_START = 20000L;

    private static final int TAXONOMY = 9999;

    private static final Timestamp CREATED_DATE = Timestamp.valueOf("2001-01-05 12:30:50.0");

    SubSnpNoHgvsToClusteredVariantProcessor processor;

    @Before
    public void setUp() throws Exception {
        processor = new SubSnpNoHgvsToClusteredVariantProcessor();
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void transformSnpForwardOrientations() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);
        // TODO: validated, match assembly and RS variant accession are being added into PR #28

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
        // TODO: compare createdDate and hash, as they are not compared in the equals method
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpClusteredVariantEntity dbsnpSubmittedVariant,
                                        VariantType expectedType) {
        this.assertProcessedVariant(subSnpNoHgvs, dbsnpSubmittedVariant, expectedType, false
        );
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpClusteredVariantEntity dbsnpSubmittedVariant,
                                        VariantType expectedType, boolean expectedValidated) {

        assertEquals(subSnpNoHgvs.getRsId(), dbsnpSubmittedVariant.getAccession());
        assertEquals(ASSEMBLY, dbsnpSubmittedVariant.getAssemblyAccession());
        assertEquals(TAXONOMY, dbsnpSubmittedVariant.getTaxonomyAccession());
        assertEquals(CHROMOSOME, dbsnpSubmittedVariant.getContig());
        assertEquals(CHROMOSOME_START, dbsnpSubmittedVariant.getStart());
        assertEquals(expectedType, dbsnpSubmittedVariant.getType());
        assertEquals(expectedValidated, dbsnpSubmittedVariant.isValidated());
        assertEquals(1, dbsnpSubmittedVariant.getVersion());
    }

    @Test
    public void transformSnpReverseSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1984788946L, 14718243L, "T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);

    }

    @Test
    public void transformSnpReverseRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "G", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformSnpReverseRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1982511850L, 14730808L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "G", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(181534645L, 14797051L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContigAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(823297358L, 14797051L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.FORWARD, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContigAndRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1979073615L, 10723963L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContigRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(822765305L, 14510048L, "C/G", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.REVERSE, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformInsertion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "-/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "-", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.INS);
    }

    @Test
    public void transformDeletion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "T/-", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.DIV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.DEL);
    }

    @Test
    public void transformDeletionReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, "T/-", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.DIV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.DEL);
    }

    @Test
    public void transformVariantSupportedByEvidence() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25945162L, 14730808L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, true, "G", CREATED_DATE, CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV, true);
    }

    @Test
    public void transformMultiallelicSS() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1975823489L, 13637891L, "A/C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpClass.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, CONTIG_START, false, false, "A", CREATED_DATE,
                                                     CREATED_DATE, TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), VariantType.SNV);
    }

    @Test
    public void transformMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(666079762L, 313826846L, "TTA/ATC", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpClass.MVN, Orientation.FORWARD, Orientation.REVERSE,
                                                     Orientation.REVERSE, CONTIG_START, false, false, "TAA",
                                                     CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.MNV);
    }

    @Test
    public void transformMultiallelicMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(973513424L, 525103154L, "AC/TC/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpClass.MVN, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, CONTIG_START, false, false, "AC",
                                                     CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.MNV);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), VariantType.MNV);
    }

    @Test
    public void leadingSlashInAllelesIsIgnored() throws Exception {
        // forward strand
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                                     CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "C", CREATED_DATE, CREATED_DATE, TAXONOMY);
        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.REVERSE,
                                        Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false, false, "A",
                                        CREATED_DATE, CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void trailingSlashInAllelesIsIgnored() throws Exception {
        // forward strabd
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                                     CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "C", CREATED_DATE, CREATED_DATE, TAXONOMY);
        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "T/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV, Orientation.REVERSE,
                                        Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false, false, "A",
                                        CREATED_DATE, CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformShortTandemRepeat() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "(T)4/5/7", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpClass.MICROSATELLITE, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "T", CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.TANDEM_REPEAT);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), VariantType.TANDEM_REPEAT);
    }

    @Test
    public void transformVariantWithNoChromosomeCoordinates() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25962272L, 14745629L, "A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     null, null, CONTIG_NAME, DbsnpClass.MVN, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "T", CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
    }

    @Test
    public void transformSnpNotMatchingAlleles() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpClass.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpClusteredVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), VariantType.SNV);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), VariantType.SNV);
    }
}
