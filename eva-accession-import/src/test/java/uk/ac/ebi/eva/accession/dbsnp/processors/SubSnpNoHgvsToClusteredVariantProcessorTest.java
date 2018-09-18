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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.sql.Timestamp;

import static org.junit.Assert.assertEquals;

public class SubSnpNoHgvsToClusteredVariantProcessorTest {

    private static final String ASSEMBLY = "AnyAssembly-1.0";

    private static final String ASSEMBLY_ACCESSION = "GCA_123456789";

    private static final String PROJECT_ACCESSION = "HANDLE_NAME";

    private static final String BATCH_HANDLE = "HANDLE";

    private static final String BATCH_NAME = "NAME";

    private static final String CHROMOSOME = "10";

    private static final long CHROMOSOME_START = 1000000L;

    private static final String CONTIG_NAME = "Contig1";

    private static final long CONTIG_START = 20000L;

    private static final int TAXONOMY = 9999;

    private static final Timestamp SS_CREATED_DATE = Timestamp.valueOf("2000-05-10 13:10:40.0");
    
    private static final Timestamp RS_CREATED_DATE = Timestamp.valueOf("2001-01-05 12:30:50.0");

    private SubSnpNoHgvsToClusteredVariantProcessor processor;

    @Before
    public void setUp() throws Exception {
        processor = new SubSnpNoHgvsToClusteredVariantProcessor(ASSEMBLY_ACCESSION);
    }

    @After
    public void tearDown() throws Exception {
    }

    @Test
    public void transformSnpForwardOrientations() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpClusteredVariantEntity dbsnpSubmittedVariant,
                                        VariantType expectedType) {
        this.assertProcessedVariant(subSnpNoHgvs, dbsnpSubmittedVariant, expectedType, false, CONTIG_NAME,
                                    CONTIG_START);
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpClusteredVariantEntity dbsnpClusteredVariant,
                                        VariantType expectedType, boolean expectedValidated, String chromosome,
                                        long chromosomeStart) {

        assertEquals(subSnpNoHgvs.getRsId(), dbsnpClusteredVariant.getAccession());
        assertEquals(ASSEMBLY_ACCESSION, dbsnpClusteredVariant.getAssemblyAccession());
        assertEquals(TAXONOMY, dbsnpClusteredVariant.getTaxonomyAccession());
        assertEquals(chromosome, dbsnpClusteredVariant.getContig());
        assertEquals(chromosomeStart, dbsnpClusteredVariant.getStart());
        assertEquals(expectedType, dbsnpClusteredVariant.getType());
        assertEquals(expectedValidated, dbsnpClusteredVariant.isValidated());
        assertEquals(1, dbsnpClusteredVariant.getVersion());
        assertEquals(getExpectedHash(chromosome, chromosomeStart, expectedType),
                     dbsnpClusteredVariant.getHashedMessage());
        assertEquals(RS_CREATED_DATE.toLocalDateTime(), dbsnpClusteredVariant.getCreatedDate());
    }

    public String getExpectedHash(String contig, long start, VariantType type) {
        String summary = new StringBuilder()
                .append(ASSEMBLY_ACCESSION)
                .append("_").append(TAXONOMY)
                .append("_").append(contig)
                .append("_").append(start)
                .append("_").append(type)
                .toString();
        return new SHA1HashingFunction().apply(summary);
    }

    @Test
    public void transformSnpReverseSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1984788946L, 14718243L, "A", "T/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformSnpReverseRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "G", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformSnpReverseRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1982511850L, 14730808L, "G", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(181534645L, 14797051L, "T", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContigAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(823297358L, 14797051L, "T", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.FORWARD, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContigAndRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1979073615L, 10723963L, "C", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformSnpReverseContigRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(822765305L, 14510048L, "C", "C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformInsertion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "-", "-/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.INS);
    }

    @Test
    public void transformDeletionAlternatesDifferentType() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "C", "-/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.DEL);
    }

    @Test
    public void transformMnv() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "C", "T/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.MNV);
    }

    @Test
    public void transformDeletion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "T", "T/-", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.DEL);
    }

    @Test
    public void transformDeletionReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, "A", "T/-", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.DEL);
    }

    @Test
    public void transformVariantSupportedByEvidence() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25945162L, 14730808L, "G", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, false, false,
                                                     true, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformMultiallelicSS() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1975823489L, 13637891L, "A", "A/C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(666079762L, 313826846L, "TAA", "TTA/ATC", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.MNV);
    }

    @Test
    public void transformMultiallelicMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(973513424L, 525103154L, "AC", "AC/TC/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.MNV);
    }

    @Test
    public void leadingSlashInAllelesIsIgnored() throws Exception {
        // forward strand
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "C", "/A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                                     RS_CREATED_DATE, TAXONOMY);
        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A", "/T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.SNV,
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);
        variant = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void trailingSlashInAllelesIsIgnored() throws Exception {
        // forward strabd
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "C", "A/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                                     RS_CREATED_DATE, TAXONOMY);
        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A", "T/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.SNV,
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);
        variant = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }

    @Test
    public void transformShortTandemRepeat() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "T", "(T)4/5/7", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.TANDEM_REPEAT);
    }

    @Test
    public void transformVariantWithNoChromosomeCoordinates() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25962272L, 14745629L, "T", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, null, null, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.MNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     false, false, false, false, SS_CREATED_DATE, RS_CREATED_DATE,
                                                     TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.MNV, false, CONTIG_NAME, CONTIG_START);
    }

    @Test
    public void transformSnpNotMatchingAlleles() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "T", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpClusteredVariantEntity variant = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variant, VariantType.SNV);
    }
}
