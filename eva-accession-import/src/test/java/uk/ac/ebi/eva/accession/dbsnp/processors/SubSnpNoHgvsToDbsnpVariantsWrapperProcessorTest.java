/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static uk.ac.ebi.eva.accession.dbsnp.processors.SubSnpNoHgvsToDbsnpVariantsWrapperProcessor.DECLUSTERED;
import static uk.ac.ebi.eva.accession.dbsnp.processors.SubSnpNoHgvsToDbsnpVariantsWrapperProcessor.DECLUSTERED_ALLELES_MISMATCH;
import static uk.ac.ebi.eva.accession.dbsnp.processors.SubSnpNoHgvsToDbsnpVariantsWrapperProcessor.DECLUSTERED_TYPE_MISMATCH;

public class SubSnpNoHgvsToDbsnpVariantsWrapperProcessorTest {

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

    private static SubSnpNoHgvsToDbsnpVariantsWrapperProcessor processor;

    private static FastaSequenceReader fastaSequenceReader;

    @BeforeClass
    public static void setUpClass() throws Exception {
        fastaSequenceReader = new FastaSequenceReader(Paths.get("src/test/resources/Gallus_gallus-5.0.test.fa"));
        processor = new SubSnpNoHgvsToDbsnpVariantsWrapperProcessor(fastaSequenceReader);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        fastaSequenceReader.close();
    }

    @Test
    public void processSubmittedVariantAllelesMismatch() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, CREATED_DATE,
                                                     CREATED_DATE, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvs);

        //Check Decluster
        assertEquals(1, dbsnpVariantsWrapper.getSubmittedVariants().size());
        assertNull(dbsnpVariantsWrapper.getSubmittedVariants().get(0).getClusteredVariantAccession());

        //Check Operations
        assertOperations(dbsnpVariantsWrapper, getReason(Collections.singletonList(DECLUSTERED_ALLELES_MISMATCH)),
                         subSnpNoHgvs.getSsId(), 0);

        //Check Rs
        assertEquals(subSnpNoHgvs.getRsId(), dbsnpVariantsWrapper.getClusteredVariant().getAccession());
    }

    private String getReason(List<String> reasons){
        StringBuilder reason = new StringBuilder(DECLUSTERED);
        reasons.forEach(reason::append);
        return reason.toString();
    }

    @Test
    public void processSubmittedVariantTypeMismatch() throws Exception {
        SubSnpNoHgvs subSnpNoHgvsTypeMismatch = new SubSnpNoHgvs(25928972L, 14718243L, "G", "G/-/T/TT", ASSEMBLY,
                                                                 BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START,
                                                                 CONTIG_NAME, CONTIG_START, DbsnpVariantType.DIV,
                                                                 Orientation.FORWARD, Orientation.FORWARD,
                                                                 Orientation.FORWARD, false, false, false, false,
                                                                 CREATED_DATE, CREATED_DATE, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvsTypeMismatch);

        //Check Decluster
        assertEquals(3, dbsnpVariantsWrapper.getSubmittedVariants().size());
        assertNotNull(dbsnpVariantsWrapper.getSubmittedVariants().get(0).getClusteredVariantAccession());
        assertNull(dbsnpVariantsWrapper.getSubmittedVariants().get(1).getClusteredVariantAccession());
        assertNull(dbsnpVariantsWrapper.getSubmittedVariants().get(2).getClusteredVariantAccession());

        //Check Operations
        assertEquals(2, dbsnpVariantsWrapper.getOperations().size());
        assertOperations(dbsnpVariantsWrapper, getReason(Collections.singletonList(DECLUSTERED_TYPE_MISMATCH)),
                         subSnpNoHgvsTypeMismatch.getSsId(), 0);
        assertOperations(dbsnpVariantsWrapper, getReason(Collections.singletonList(DECLUSTERED_TYPE_MISMATCH)),
                         subSnpNoHgvsTypeMismatch.getSsId(), 1);

        //Check Rs
        assertEquals(subSnpNoHgvsTypeMismatch.getRsId(), dbsnpVariantsWrapper.getClusteredVariant().getAccession());
    }

    @Test
    public void processSubmittedVariantAllelesAndTypeMismatch() throws Exception {
        SubSnpNoHgvs subSnpNoHgvsAlleleAndTypeMismatch = new SubSnpNoHgvs(25928972L, 14718243L, "G", "-/T/TT", ASSEMBLY,
                                                                          BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                                                          CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                                          DbsnpVariantType.DIV, Orientation.FORWARD,
                                                                          Orientation.FORWARD, Orientation.FORWARD,
                                                                          false, false, false, false, CREATED_DATE,
                                                                          CREATED_DATE, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvsAlleleAndTypeMismatch);

        //Check Decluster
        assertEquals(3, dbsnpVariantsWrapper.getSubmittedVariants().size());
        assertNull(dbsnpVariantsWrapper.getSubmittedVariants().get(0).getClusteredVariantAccession());
        assertNull(dbsnpVariantsWrapper.getSubmittedVariants().get(1).getClusteredVariantAccession());
        assertNull(dbsnpVariantsWrapper.getSubmittedVariants().get(2).getClusteredVariantAccession());

        //Check Operations
        assertEquals(3, dbsnpVariantsWrapper.getOperations().size());
        assertOperations(dbsnpVariantsWrapper, getReason(Collections.singletonList(DECLUSTERED_ALLELES_MISMATCH)),
                         subSnpNoHgvsAlleleAndTypeMismatch.getSsId(), 0);
        assertOperations(dbsnpVariantsWrapper, getReason(Arrays.asList(DECLUSTERED_ALLELES_MISMATCH,
                                                                       DECLUSTERED_TYPE_MISMATCH)),
                                                         subSnpNoHgvsAlleleAndTypeMismatch.getSsId(), 1);
        assertOperations(dbsnpVariantsWrapper, getReason(Arrays.asList(DECLUSTERED_ALLELES_MISMATCH,
                                                                       DECLUSTERED_TYPE_MISMATCH)),
                         subSnpNoHgvsAlleleAndTypeMismatch.getSsId(), 2);

        //Check Rs
        assertEquals(subSnpNoHgvsAlleleAndTypeMismatch.getRsId(), dbsnpVariantsWrapper.getClusteredVariant().getAccession());
    }

    private void assertOperations(DbsnpVariantsWrapper dbsnpVariantsWrapper, String reason, Long ss, int index) {
        assertEquals(EventType.UPDATED, dbsnpVariantsWrapper.getOperations().get(index).getEventType());
        assertEquals(1, dbsnpVariantsWrapper.getOperations().get(index).getInactiveObjects().size());
        assertEquals(reason, dbsnpVariantsWrapper.getOperations().get(index).getReason());
        assertEquals(ss, dbsnpVariantsWrapper.getOperations().get(index).getAccession());
    }

    @Test
    public void transformSnpForwardOrientations() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "C");
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpSubmittedVariantEntity dbsnpSubmittedVariant,
                                        String expectedReference, String expectedAlternate) {
        this.assertProcessedVariant(subSnpNoHgvs, dbsnpSubmittedVariant, CHROMOSOME, CHROMOSOME_START,
                                    expectedReference, expectedAlternate, false, true, 1);
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpSubmittedVariantEntity dbsnpSubmittedVariant,
                                        String expectedContig, long expectedStart, String expectedReference,
                                        String expectedAlternate, boolean supportedByEvidence, boolean allelesMatch,
                                        int expectedVersion) {
        assertEquals(subSnpNoHgvs.getSsId(), dbsnpSubmittedVariant.getAccession());
//      TODO: Rs must be validated after ticket EVA-1278 is finished - ie. (T)4 must be TTTT
//        assertEquals(subSnpNoHgvs.getRsId(), dbsnpSubmittedVariant.getClusteredVariantAccession());
        assertEquals(ASSEMBLY, dbsnpSubmittedVariant.getAssemblyAccession());
        assertEquals(TAXONOMY, dbsnpSubmittedVariant.getTaxonomyAccession());
        assertEquals(PROJECT_ACCESSION, dbsnpSubmittedVariant.getProjectAccession());
        assertEquals(expectedContig, dbsnpSubmittedVariant.getContig());
        assertEquals(expectedStart, dbsnpSubmittedVariant.getStart());
        assertEquals(expectedReference, dbsnpSubmittedVariant.getReferenceAllele());
        assertEquals(expectedAlternate, dbsnpSubmittedVariant.getAlternateAllele());
        assertEquals(supportedByEvidence, dbsnpSubmittedVariant.isSupportedByEvidence());
        assertEquals(false, dbsnpSubmittedVariant.isAssemblyMatch());
        assertEquals(allelesMatch, dbsnpSubmittedVariant.isAllelesMatch());
        assertEquals(false, dbsnpSubmittedVariant.isValidated());
        assertEquals(CREATED_DATE.toLocalDateTime(), dbsnpSubmittedVariant.getCreatedDate());
        assertEquals(expectedVersion, dbsnpSubmittedVariant.getVersion());
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
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1984788946L, 14718243L, "A", "T/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");

    }

    @Test
    public void transformSnpReverseRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "G", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1982511850L, 14730808L, "G", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(181534645L, 14797051L, "T", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformSnpReverseContigAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(823297358L, 14797051L, "T", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.FORWARD, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformSnpReverseContigAndRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1979073615L, 10723963L, "C", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseContigRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(822765305L, 14510048L, "C", "C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "C");
    }

    @Test
    public void transformInsertion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "-", "-/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "", "T");

        subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "C", "G/AG", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                        CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.DIV,
                                        Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE, false, false,
                                        false, false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "AG");
    }

    @Test
    public void transformDeletion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "T", "T/-", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "T", "");

        subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "TG", "TG/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                        CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.DIV,
                                        Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "TG", "T");
    }

    @Test
    public void transformIndel() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052218848L, 794686157L, "G", "C/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "TA");
    }

    @Test
    public void transformDeletionReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, "A", "T/-", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "T", "");
    }

    @Test
    public void transformVariantSupportedByEvidence() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25945162L, 14730808L, "G", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, true, false, true,
                                                     CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "G", "A", true, true, 1);

        // variant with genotypes but no frequencies
        subSnpNoHgvs.setFrequencyExists(false);
        subSnpNoHgvs.setGenotypeExists(true);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "G", "A", true, true, 1);

        // variant with frequencies but no genotypes
        subSnpNoHgvs.setFrequencyExists(true);
        subSnpNoHgvs.setGenotypeExists(false);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "G", "A", true, true, 1);
    }

    @Test
    public void transformMultiallelicSS() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1975823489L, 13637891L, "A", "A/C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "A", "C", false, true, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "A", "G", false, true, 1);
    }

    @Test
    public void transformMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(666079762L, 313826846L, "TAA", "TTA/ATC", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "TTA", "ATC");
    }

    @Test
    public void transformMultiallelicMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(973513424L, 525103154L, "AC", "AC/TC/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "AC", "TC", false, true, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "AC", "TA", false, true, 1);
    }

    @Test
    public void leadingSlashInAllelesIsIgnored() throws Exception {
        // forward strand
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "C", "/A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, CREATED_DATE,
                                                     CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "A");

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A", "/T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.SNV,
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, CREATED_DATE, CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void trailingSlashInAllelesIsIgnored() throws Exception {
        // forward strand
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "C", "A/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, CREATED_DATE,
                                                     CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "A");

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A", "T/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.SNV,
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, CREATED_DATE, CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformShortTandemRepeat() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "T", "(T)4/5/7", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "T", "TTTT", false, false,
                               1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "T", "TTTTT", false, false,
                               1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(2), CHROMOSOME, CHROMOSOME_START, "T", "TTTTTTT", false,
                               false, 1);

        subSnpNoHgvs = new SubSnpNoHgvs(244316767L, 315216130L, "TA", "(TA)14(CA)2TA/-", ASSEMBLY, BATCH_HANDLE,
                                        BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                        DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD, Orientation.REVERSE,
                                        Orientation.REVERSE, false, false, false, false, CREATED_DATE, CREATED_DATE,
                                        TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "TA",
                               "TATATATATATATATATATATATATATACACATA", false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "TA", "", false, false, 1);

        subSnpNoHgvs = new SubSnpNoHgvs(702701141L, 718200201L, "A", "(A)2(TA)8/(A)2(TA)6/(A)2(TA)7/(A)4(TA)9",
                                        ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                        CONTIG_START, DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD,
                                        Orientation.FORWARD, Orientation.FORWARD, false, false, false, false,
                                        CREATED_DATE, CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "A", "AATATATATATATATATA",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "A", "AATATATATATATA",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(2), CHROMOSOME, CHROMOSOME_START, "A", "AATATATATATATATA",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(3), CHROMOSOME, CHROMOSOME_START, "A",
                               "AAAATATATATATATATATATA", false, false, 1);
    }

    @Test
    public void transformShortTandemRepeatReverseStrand() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(59831075L, 109775855L, "AT", "(AT)7/(AT)19", ASSEMBLY,
                                                     BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START,
                                                     CONTIG_NAME, CONTIG_START, DbsnpVariantType.MICROSATELLITE,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     false, false, false, false, CREATED_DATE, CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "AT", "ATATATATATATAT",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "AT",
                               "ATATATATATATATATATATATATATATATATATATAT", false, false, 1);
    }

    @Test
    public void transformMicrosatelliteCornerCases() throws Exception {
        // There are STR variants that don't have repeated motifs
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "C", "C/AGAGTTCAAGTTGCCCTA", ASSEMBLY,
                                                     BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START,
                                                     CONTIG_NAME, CONTIG_START, DbsnpVariantType.MICROSATELLITE,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     false, false, false, false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "C", "AGAGTTCAAGTTGCCCTA",
                               false, true, 1);

        // there are STR variants where the alleles string is enclosed with square brackets
        subSnpNoHgvs = new SubSnpNoHgvs(159831041L, 109548685L, "ACACACACAC", "[(GT)11/14]", ASSEMBLY, BATCH_HANDLE,
                                        BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                        DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD, Orientation.REVERSE,
                                        Orientation.FORWARD, false, false, false, false, CREATED_DATE, CREATED_DATE,
                                        TAXONOMY);

        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "ACACACACAC",
                               "ACACACACACACACACACACAC", false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "ACACACACAC",
                               "ACACACACACACACACACACACACACAC", false, false, 1);
    }

    @Test
    public void transformVariantWithNoChromosomeCoordinates() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25962272L, 14745629L, "T", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, null, null, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.REVERSE,
                                                     Orientation.FORWARD, false, false, false, false, CREATED_DATE,
                                                     CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "T", "G", false, true, 1);
    }

    @Test
    public void transformSnpNotMatchingAlleles() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "T", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, CREATED_DATE, CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CHROMOSOME, CHROMOSOME_START, "T", "A", false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CHROMOSOME, CHROMOSOME_START, "T", "C", false, false, 1);
    }

}
