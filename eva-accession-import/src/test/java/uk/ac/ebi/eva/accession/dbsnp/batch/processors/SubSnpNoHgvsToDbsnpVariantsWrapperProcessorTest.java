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
package uk.ac.ebi.eva.accession.dbsnp.batch.processors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.Orientation;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.model.ProjectAccessionMapping;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

public class SubSnpNoHgvsToDbsnpVariantsWrapperProcessorTest {

    private static final String ASSEMBLY = "AnyAssembly-1.0";

    private static final String ASSEMBLY_ACCESSION = "GCA_123456789";

    private static final String PROJECT_ACCESSION = "HANDLE_NAME";

    private static final String BATCH_HANDLE = "HANDLE";

    private static final String BATCH_NAME = "NAME";

    private static final String CHROMOSOME = "10";

    private static final String CHROMOSOME_2 = "22";

    private static final long CHROMOSOME_START = 1000000L;

    private static final String CONTIG_NAME = "Contig1";

    private static final long CONTIG_START = 20000L;

    private static final int TAXONOMY = 9999;

    private static final Timestamp SS_CREATED_DATE = Timestamp.valueOf("2000-05-10 13:10:40.0");

    private static final Timestamp RS_CREATED_DATE = Timestamp.valueOf("2001-01-05 12:30:50.0");

    private static SubSnpNoHgvsToDbsnpVariantsWrapperProcessor processor;

    private static FastaSynonymSequenceReader fastaSynonymSequenceReader;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path fastaPath = Paths.get("src/test/resources/input-files/fasta/Gallus_gallus-5.0.test.fa");
        ContigMapping contigMapping = new ContigMapping(Collections.emptyList());
        fastaSynonymSequenceReader = new FastaSynonymSequenceReader(contigMapping, fastaPath);
        processor = new SubSnpNoHgvsToDbsnpVariantsWrapperProcessor(ASSEMBLY_ACCESSION, fastaSynonymSequenceReader,
                                                                    Collections.emptyList());
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        fastaSynonymSequenceReader.close();
    }

    @Test
    public void processSubSnpNoHgvsToWrapper() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                                     RS_CREATED_DATE, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvs);

        assertEquals(1, dbsnpVariantsWrapper.getSubmittedVariants().size());
        assertNotNull(dbsnpVariantsWrapper.getClusteredVariant());
        assertNull(dbsnpVariantsWrapper.getOperations());
        assertEquals(DbsnpVariantType.SNV, dbsnpVariantsWrapper.getDbsnpVariantType());
    }

    @Test
    public void processSubSnpNoHgvsToWrapperMultipleAlternates() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "G/C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvs);

        assertEquals(3, dbsnpVariantsWrapper.getSubmittedVariants().size());
        assertNotNull(dbsnpVariantsWrapper.getClusteredVariant());
        assertNull(dbsnpVariantsWrapper.getOperations());
        assertEquals(DbsnpVariantType.SNV, dbsnpVariantsWrapper.getDbsnpVariantType());

        assertEquals("G", dbsnpVariantsWrapper.getSubmittedVariants().get(0).getAlternateAllele());
        assertEquals("C", dbsnpVariantsWrapper.getSubmittedVariants().get(1).getAlternateAllele());
        assertEquals("T", dbsnpVariantsWrapper.getSubmittedVariants().get(2).getAlternateAllele());
    }

    @Test
    public void processSubSnpNoHgvsToWrapperReferenceInAlternates() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "A/C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvs);

        assertEquals(2, dbsnpVariantsWrapper.getSubmittedVariants().size());
        assertNotNull(dbsnpVariantsWrapper.getClusteredVariant());
        assertNull(dbsnpVariantsWrapper.getOperations());
        assertEquals(DbsnpVariantType.SNV, dbsnpVariantsWrapper.getDbsnpVariantType());

        assertEquals("C", dbsnpVariantsWrapper.getSubmittedVariants().get(0).getAlternateAllele());
        assertEquals("T", dbsnpVariantsWrapper.getSubmittedVariants().get(1).getAlternateAllele());
    }

    @Test
    public void transformSnpForwardOrientations() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "C");
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpSubmittedVariantEntity dbsnpSubmittedVariant,
                                        String expectedReference, String expectedAlternate) {
        this.assertProcessedVariant(subSnpNoHgvs, dbsnpSubmittedVariant, CONTIG_NAME, CONTIG_START,
                                    expectedReference, expectedAlternate, false, true, 1);
    }

    private void assertProcessedVariant(SubSnpNoHgvs subSnpNoHgvs, DbsnpSubmittedVariantEntity dbsnpSubmittedVariant,
                                        String expectedContig, long expectedStart, String expectedReference,
                                        String expectedAlternate, boolean supportedByEvidence, boolean allelesMatch,
                                        int expectedVersion) {
        assertEquals(subSnpNoHgvs.getSsId(), dbsnpSubmittedVariant.getAccession());
        assertEquals(subSnpNoHgvs.getRsId(), dbsnpSubmittedVariant.getClusteredVariantAccession());
        assertEquals(ASSEMBLY_ACCESSION, dbsnpSubmittedVariant.getReferenceSequenceAccession());
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
        assertEquals(SS_CREATED_DATE.toLocalDateTime(), dbsnpSubmittedVariant.getCreatedDate());
        assertEquals(expectedVersion, dbsnpSubmittedVariant.getVersion());
        assertEquals(getExpectedHash(expectedContig, expectedStart, expectedReference, expectedAlternate),
                     dbsnpSubmittedVariant.getHashedMessage());
    }

    public String getExpectedHash(String contig, long start, String reference, String alternate) {
        String summary = new StringBuilder()
                .append(ASSEMBLY_ACCESSION)
                .append("_").append(PROJECT_ACCESSION)
                .append("_").append(contig)
                .append("_").append(start)
                .append("_").append(reference)
                .append("_").append(alternate)
                .toString();
        return new SHA1HashingFunction().apply(summary);
    }

    @Test
    public void mapEvaStudyId() throws Exception {
        List<ProjectAccessionMapping> projectAccessionMappings = new ArrayList<>();
        String handle = "HANDLE_TO_BE_REPLACED";
        String batchName = "BATCH_NAME_TO_BE_REPLACED";
        String evaStudyId = "EVA_STUDY_ID";
        projectAccessionMappings.add(new ProjectAccessionMapping(evaStudyId, handle, batchName, TAXONOMY));

        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "A/C", ASSEMBLY, handle,
                                                     batchName, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        processor = new SubSnpNoHgvsToDbsnpVariantsWrapperProcessor(ASSEMBLY_ACCESSION, fastaSynonymSequenceReader,
                                                                    projectAccessionMappings);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertEquals(evaStudyId, variants.get(0).getProjectAccession());
    }

    @Test
    public void transformSnpReverseSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1984788946L, 14718243L, "A", "T/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");

    }

    @Test
    public void transformSnpReverseRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(186667770L, 14730808L, "G", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1982511850L, 14730808L, "G", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(181534645L, 14797051L, "T", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformSnpReverseContigAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(823297358L, 14797051L, "T", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.FORWARD, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformSnpReverseContigAndRs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1979073615L, 10723963L, "C", "G/A", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "A");
    }

    @Test
    public void transformSnpReverseContigRsAndSs() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(822765305L, 14510048L, "C", "C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.REVERSE,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "C");
    }

    @Test
    public void transformDeletionUnknownOrientationSetsAlleleMismatch() throws Exception {
        String reference = "A";
        long position = 7L;
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, reference, "T/-", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME_2, position, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.UNKNOWN, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertEquals(1, variants.size());
        boolean allelesMatch = false;
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, reference, "", false,
                               allelesMatch, 1);
    }

    @Test
    public void transformInsertion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "-", "-/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "", "T");

        subSnpNoHgvs = new SubSnpNoHgvs(1052228949L, 794529293L, "C", "G/AG", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                        CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.DIV,
                                        Orientation.FORWARD, Orientation.REVERSE, Orientation.REVERSE, false, false,
                                        false, false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "G", "AG");
    }

    @Test
    public void transformDeletion() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "T", "T/-", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "T", "");

        subSnpNoHgvs = new SubSnpNoHgvs(808112673L, 794532822L, "TG", "TG/T", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                        CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.DIV,
                                        Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "TG", "T");
    }

    @Test
    public void transformIndel() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1052218848L, 794686157L, "G", "C/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "TA");
    }

    @Test
    public void transformDeletionReverseContig() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(820982442L, 794525917L, "A", "T/-", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.DIV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "T", "");
    }

    @Test
    public void transformVariantSupportedByEvidence() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25945162L, 14730808L, "G", "C/T", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.FORWARD, false, true, false, true,
                                                     SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "G", "A", true, true, 1);

        // variant with genotypes but no frequencies
        subSnpNoHgvs.setFrequencyExists(false);
        subSnpNoHgvs.setGenotypeExists(true);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "G", "A", true, true, 1);

        // variant with frequencies but no genotypes
        subSnpNoHgvs.setFrequencyExists(true);
        subSnpNoHgvs.setGenotypeExists(false);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "G", "A", true, true, 1);
    }

    @Test
    public void transformMultiallelicSS() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1975823489L, 13637891L, "A", "A/C/G", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "A", "C", false, true, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "A", "G", false, true, 1);
    }

    @Test
    public void transformMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(666079762L, 313826846L, "TAA", "TTA/ATC", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MNV, Orientation.FORWARD,
                                                     Orientation.REVERSE, Orientation.REVERSE, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "TTA", "ATC");
    }

    @Test
    public void transformMultiallelicMNV() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(973513424L, 525103154L, "AC", "AC/TC/TA", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "AC", "TC", false, true, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "AC", "TA", false, true, 1);
    }

    @Test
    public void leadingSlashInAllelesIsIgnored() throws Exception {
        // forward strand
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "C", "/A/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                                     RS_CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "A");

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A", "/T/C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.SNV,
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void trailingSlashInAllelesIsIgnored() throws Exception {
        // forward strand
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "C", "A/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                                     RS_CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "C", "A");

        // reverse strand
        subSnpNoHgvs = new SubSnpNoHgvs(1L, 2L, "A", "T/C/", ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME,
                                        CHROMOSOME_START, CONTIG_NAME, CONTIG_START, DbsnpVariantType.SNV,
                                        Orientation.REVERSE, Orientation.FORWARD, Orientation.FORWARD, false, false,
                                        false, false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), "A", "G");
    }

    @Test
    public void transformShortTandemRepeat() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "T", "(T)4/5/7", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "T", "TTTT", false, false,
                               1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "T", "TTTTT", false, false,
                               1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(2), CONTIG_NAME, CONTIG_START, "T", "TTTTTTT", false,
                               false, 1);

        subSnpNoHgvs = new SubSnpNoHgvs(244316767L, 315216130L, "TA", "(TA)14(CA)2TA/-", ASSEMBLY, BATCH_HANDLE,
                                        BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                        DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD, Orientation.REVERSE,
                                        Orientation.REVERSE, false, false, false, false, SS_CREATED_DATE,
                                        RS_CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "TA",
                               "TATATATATATATATATATATATATATACACATA", false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "TA", "", false, false, 1);

        subSnpNoHgvs = new SubSnpNoHgvs(702701141L, 718200201L, "A", "(A)2(TA)8/(A)2(TA)6/(A)2(TA)7/(A)4(TA)9",
                                        ASSEMBLY, BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                        CONTIG_START, DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD,
                                        Orientation.FORWARD, Orientation.FORWARD, false, false, false, false,
                                        SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);
        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "A", "AATATATATATATATATA",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "A", "AATATATATATATA",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(2), CONTIG_NAME, CONTIG_START, "A", "AATATATATATATATA",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(3), CONTIG_NAME, CONTIG_START, "A",
                               "AAAATATATATATATATATATA", false, false, 1);
    }

    @Test
    public void transformShortTandemRepeatReverseStrand() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(59831075L, 109775855L, "AT", "(AT)7/(AT)19", ASSEMBLY,
                                                     BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START,
                                                     CONTIG_NAME, CONTIG_START, DbsnpVariantType.MICROSATELLITE,
                                                     Orientation.FORWARD, Orientation.REVERSE, Orientation.FORWARD,
                                                     false, false, false, false, SS_CREATED_DATE, RS_CREATED_DATE,
                                                     TAXONOMY);
        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "AT", "ATATATATATATAT",
                               false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "AT",
                               "ATATATATATATATATATATATATATATATATATATAT", false, false, 1);
    }

    @Test
    public void transformMicrosatelliteCornerCases() throws Exception {
        // There are STR variants that don't have repeated motifs
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(700214256L, 439714840L, "C", "C/AGAGTTCAAGTTGCCCTA", ASSEMBLY,
                                                     BATCH_HANDLE, BATCH_NAME, CHROMOSOME, CHROMOSOME_START,
                                                     CONTIG_NAME, CONTIG_START, DbsnpVariantType.MICROSATELLITE,
                                                     Orientation.FORWARD, Orientation.FORWARD, Orientation.FORWARD,
                                                     false, false, false, false, SS_CREATED_DATE, RS_CREATED_DATE,
                                                     TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "C", "AGAGTTCAAGTTGCCCTA",
                               false, true, 1);

        // there are STR variants where the alleles string is enclosed with square brackets
        subSnpNoHgvs = new SubSnpNoHgvs(159831041L, 109548685L, "ACACACACAC", "[(GT)11/14]", ASSEMBLY, BATCH_HANDLE,
                                        BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                        DbsnpVariantType.MICROSATELLITE, Orientation.FORWARD, Orientation.REVERSE,
                                        Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                        RS_CREATED_DATE, TAXONOMY);

        variants = processor.process(subSnpNoHgvs).getSubmittedVariants();
        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "ACACACACAC",
                               "ACACACACACACACACACACAC", false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "ACACACACAC",
                               "ACACACACACACACACACACACACACAC", false, false, 1);
    }

    @Test
    public void transformVariantWithNoChromosomeCoordinates() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25962272L, 14745629L, "T", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, null, null, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.REVERSE,
                                                     Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                                     RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "T", "G", false, true, 1);
    }

    @Test
    public void transformSnpNotMatchingAlleles() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "T", "A/C", ASSEMBLY, BATCH_HANDLE,
                                                     BATCH_NAME, CHROMOSOME, CHROMOSOME_START, CONTIG_NAME,
                                                     CONTIG_START, DbsnpVariantType.SNV, Orientation.FORWARD,
                                                     Orientation.FORWARD, Orientation.FORWARD, false, false, false,
                                                     false, SS_CREATED_DATE, RS_CREATED_DATE, TAXONOMY);

        List<DbsnpSubmittedVariantEntity> variants = processor.process(subSnpNoHgvs).getSubmittedVariants();

        assertProcessedVariant(subSnpNoHgvs, variants.get(0), CONTIG_NAME, CONTIG_START, "T", "A", false, false, 1);
        assertProcessedVariant(subSnpNoHgvs, variants.get(1), CONTIG_NAME, CONTIG_START, "T", "C", false, false, 1);
    }

    @Test
    public void processSubSnpNullDate() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, null,
                                                     RS_CREATED_DATE, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvs);
        assertEquals(RS_CREATED_DATE.toLocalDateTime(), dbsnpVariantsWrapper.getClusteredVariant().getCreatedDate());
        dbsnpVariantsWrapper.getSubmittedVariants().stream().forEach(
                ss -> assertEquals(RS_CREATED_DATE.toLocalDateTime(), ss.getCreatedDate()));
    }

    @Test
    public void processRefSnpNullDate() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, SS_CREATED_DATE,
                                                     null, TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvs);
        assertEquals(SS_CREATED_DATE.toLocalDateTime(), dbsnpVariantsWrapper.getClusteredVariant().getCreatedDate());
        dbsnpVariantsWrapper.getSubmittedVariants().stream().forEach(
                ss -> assertEquals(SS_CREATED_DATE.toLocalDateTime(), ss.getCreatedDate()));
    }

    @Test
    public void processSubSnpAndRefSnpNullDate() throws Exception {
        SubSnpNoHgvs subSnpNoHgvs = new SubSnpNoHgvs(25928972L, 14718243L, "A", "C", ASSEMBLY, BATCH_HANDLE, BATCH_NAME,
                                                     CHROMOSOME, CHROMOSOME_START, CONTIG_NAME, CONTIG_START,
                                                     DbsnpVariantType.SNV, Orientation.FORWARD, Orientation.FORWARD,
                                                     Orientation.FORWARD, false, false, false, false, null, null,
                                                     TAXONOMY);

        DbsnpVariantsWrapper dbsnpVariantsWrapper = processor.process(subSnpNoHgvs);
        assertNotNull(dbsnpVariantsWrapper.getClusteredVariant().getCreatedDate());
        dbsnpVariantsWrapper.getSubmittedVariants().stream().forEach(ss -> assertNotNull(ss.getCreatedDate()));
        dbsnpVariantsWrapper.getSubmittedVariants().stream().forEach(
                ss -> assertEquals(ss.getCreatedDate().toLocalDate(),
                                   dbsnpVariantsWrapper.getClusteredVariant().getCreatedDate().toLocalDate()));
    }

}
