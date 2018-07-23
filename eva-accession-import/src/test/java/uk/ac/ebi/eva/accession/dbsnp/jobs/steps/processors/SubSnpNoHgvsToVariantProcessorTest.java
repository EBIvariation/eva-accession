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
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpSubmittedVariantEntity;

import java.sql.Timestamp;
import java.util.List;

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

    private static final Timestamp CREATED_DATE = Timestamp.valueOf("2001-01-05 12:30:50.0");

    SubSnpNoHgvsToVariantProcessor processor;

    @Before
    public void setUp() throws Exception {
        processor = new SubSnpNoHgvsToVariantProcessor();
    }


    @Test
    public void transformSnpForwardOrientations() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "C");
        // TODO: compare createdDate and hash, as they are not compared in the equals method
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpSubmittedVariantEntity dbsnpSubmittedVariant,
                                        String expectedReference, String expectedAlternate) {
        this.assertProcessedVariant(subSnpNoHgvs, dbsnpSubmittedVariant, CHROMOSOME, CHROMOSOME_START,
                                    expectedReference, expectedAlternate, false, true);
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpSubmittedVariantEntity dbsnpSubmittedVariant,
                                        String expectedContig, long expectedStart, String expectedReference,
                                        String expectedAlternate, boolean supportedByEvidence, boolean allelesMatch) {
        assertEquals(subSnpNoHgvs.getSsId(), dbsnpSubmittedVariant.getAccession());
        assertEquals(subSnpNoHgvs.getRsId(), dbsnpSubmittedVariant.getClusteredVariantAccession());
        assertEquals(ASSEMBLY, dbsnpSubmittedVariant.getAssemblyAccession());
        assertEquals(TAXONOMY, dbsnpSubmittedVariant.getTaxonomyAccession());
        assertEquals(PROJECT_ACCESSION, dbsnpSubmittedVariant.getProjectAccession());
        assertEquals(expectedContig, dbsnpSubmittedVariant.getContig());
        assertEquals(expectedStart, dbsnpSubmittedVariant.getStart());
        assertEquals(expectedReference, dbsnpSubmittedVariant.getReferenceAllele());
        assertEquals(expectedAlternate, dbsnpSubmittedVariant.getAlternateAllele());
        assertEquals(supportedByEvidence, dbsnpSubmittedVariant.isSupportedByEvidence());
        assertEquals(true, dbsnpSubmittedVariant.isAssemblyMatch());
        assertEquals(allelesMatch, dbsnpSubmittedVariant.isAllelesMatch());
        assertEquals(false, dbsnpSubmittedVariant.isValidated());
        assertEquals(CREATED_DATE.toLocalDateTime(), dbsnpSubmittedVariant.getCreatedDate());
        assertEquals(1, dbsnpSubmittedVariant.getVersion());
        assertEquals(getExpectedHash(expectedContig, expectedStart, expectedReference, expectedAlternate),
                     dbsnpSubmittedVariant.getHashedMessage());
    }

    public String getExpectedHash(String contig, long start, String reference, String alternate) {
        String summary = new StringBuilder()
                .append(ASSEMBLY)
                .append("_").append(TAXONOMY)
                .append("_").append(PROJECT_ACCESSION)
                .append("_").append(contig)
                .append("_").append(start)
                .append("_").append(reference)
                .append("_").append(alternate)
                .toString();
        return new SHA1HashingFunction().apply(summary);
    }

    @Test
    public void transformSnpReverseSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1984788946L, 14718243L, "T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");

    }

    @Test
    public void transformSnpReverseRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "G", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1982511850L, 14730808L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.REVERSE, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "G", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(181534645L, 14797051L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformSnpReverseContigAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(823297358L, 14797051L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.REVERSE, Orientation.FORWARD, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformSnpReverseContigAndRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1979073615L, 10723963L, "G/A", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseContigRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(822765305L, 14510048L, "C/G", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.REVERSE, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "C");
    }

    @Test
    public void transformInsertion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "-/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.DIV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "-", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "-", "T");

        subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "G/AG", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                        CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.DIV,
                                        Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                        CONTIG_START, false, false, "C", CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "AG");
    }

    @Test
    public void transformDeletion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "T/-", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.DIV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "T", "-");

        subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "TG/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                        CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.DIV,
                                        Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                        CONTIG_START, false, false, "TG", CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "TG", "T");
    }

    @Test
    public void transformIndel() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052218848L, 794686157L, "C/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpVariantType.DIV, Orientation.FORWARD, Orientation.REVERSE,
                                                     Orientation.REVERSE, CONTIG_START, false, false, "G", CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "TA");
    }

    @Test
    public void transformDeletionReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, "T/-", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.DIV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE,
                                                     CONTIG_START, false, false, "A", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "T", "-");
    }

    @Test
    public void transformVariantSupportedByEvidence() throws Exception {
        // variant with genotype and frequencies
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25945162L, 14730808L, "C/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     CONTIG_START, true, true, "G", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "G", "A", true, true);

        // variant with genotypes but no frequencies
        subSnpNoHgvs.setFrequencyExists(false);
        subSnpNoHgvs.setGenotypeExists(true);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "G", "A", true, true);

        // variant with frequencies but no genotypes
        subSnpNoHgvs.setFrequencyExists(true);
        subSnpNoHgvs.setGenotypeExists(false);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "G", "A", true, true);
    }

    @Test
    public void transformMultiallelicSS() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1975823489L, 13637891L, "A/C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, CONTIG_START, false, false, "A", CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "C");
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), "A", "G");
    }

    @Test
    public void transformMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(666079762L, 313826846L, "TTA/ATC", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpVariantType.MNV, Orientation.FORWARD, Orientation.REVERSE,
                                                     Orientation.REVERSE, CONTIG_START, false, false, "TAA",
                                                     CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "TTA", "ATC");
    }

    @Test
    public void transformMultiallelicMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(973513424L, 525103154L, "AC/TC/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpVariantType.MNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, CONTIG_START, false, false, "AC",
                                                     CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "AC", "TC");
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), "AC", "TA");
    }

    @Test
    public void leadingSlashInAllelesIsIgnored() throws Exception {
        // forward strand
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                                     CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "A");

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "/T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV, Orientation.REVERSE,
                                        Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false, false, "A",
                                        CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void trailingSlashInAllelesIsIgnored() throws Exception {
        // forward strabd
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                                     CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "C", CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "A");

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "T/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV, Orientation.REVERSE,
                                        Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false, false, "A",
                                        CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformShortTandemRepeat() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "(T)4/5/7", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "T", "(T)4", false, false);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "T", "(T)5", false, false);
        assertProcessedVariant(subSnpNoHgvs, variants.get(2), CHROMOSOME, CHROMOSOME_START, "T", "(T)7", false, false);

        subSnpNoHgvs = new SubSnpNoHgvs(244316767L, 315216130L, "(TA)14(CA)2TA/-", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                        CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.MICROSATELLITE,
                                        Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE, CONTIG_START,
                                        false, false, "TA", CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "TA", "(TA)14(CA)2TA",
                               false, false);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "TA", "-", false, false);

        subSnpNoHgvs = new SubSnpNoHgvs(702701141L, 718200201L, "(A)2(TA)8/(A)2(TA)6/(A)2(TA)7/(A)4(TA)9", ASSEMBLY,
                                        BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                        DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD, Orientation.FORWARD,
                                        Orientation.FORWARD, CONTIG_START, false, false, "A", CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs);
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "A", "(A)2(TA)8", false,
                               false);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "A", "(A)2(TA)6", false,
                               false);
        assertProcessedVariant(subSnpNoHgvs, variants.get(2), CHROMOSOME, CHROMOSOME_START, "A", "(A)2(TA)7", false,
                               false);
        assertProcessedVariant(subSnpNoHgvs, variants.get(3), CHROMOSOME, CHROMOSOME_START, "A", "(A)4(TA)9", false,
                               false);
    }

    @Test
    public void transformVariantWithNoChromosomeCoordinates() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25962272L, 14745629L, "A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     null, null, CONTIG_NAME, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, CONTIG_START, false,
                                                     false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "T", "G", false, true);
    }

    @Test
    public void transformSnpNotMatchingAlleles() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, DbsnpVariantType.SNV,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     CONTIG_START, false, false, "T", CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs);

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "T", "A", false, false);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "T", "C", false, false);
    }


}