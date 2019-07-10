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
package uk.ac.ebi.eva.accession.pipeline.io;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigNaming;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.pipeline.steps.processors.ContigToGenbankReplacerProcessor.ORIGINAL_CHROMOSOME;

public class AccessionReportWriterTest {

    private static final String CONTIG_1 = "genbank_1";

    private static final String CHROMOSOME_1 = "chr1";

    private static final String CONTIG_2 = "contig_2";

    private static final int START_1 = 10;

    private static final String REFERENCE = "T";

    private static final String ALTERNATE = "A";

    private static final String CONTEXT_BASE = "G";

    private static final int TAXONOMY = 3880;

    private static final String ACCESSION_PREFIX = "ss";

    private static final long ACCESSION = 100L;

    private static final String HASH = "hash";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = null;

    private static final Boolean MATCHES_ASSEMBLY = null;

    private static final Boolean ALLELES_MATCH = null;

    private static final Boolean VALIDATED = null;

    private static final int CHROMOSOME_COLUMN_VCF = 0;

    private static final int INFO_COLUMN_VCF = 7;

    private static final String GENBANK_2 = "genbank_2";

    private static final String REFSEQ_2 = "refseq_2";

    private static final String SEQUENCE_NAME_3 = "ctg3";

    private static final String GENBANK_3 = "genbank_3";

    private static final String REFSEQ_3 = "refseq_3";

    private static final String SEQUENCE_NAME_4 = "ctg4";

    private static final String GENBANK_4 = "genbank_4";

    private static final String REFSEQ_4 = "refseq_4";

    private AccessionReportWriter accessionReportWriter;

    private File output;

    private FastaSequenceReader fastaSequenceReader;

    private ExecutionContext executionContext;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    private ContigMapping contigMapping;

    private Map<String, String> inputChromosomeToContig;

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        Path fastaPath = Paths.get(AccessionReportWriterTest.class.getResource("/input-files/fasta/mock.fa").toURI());
        contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(CHROMOSOME_1, "assembled-molecule", "1", CONTIG_1, "refseq_1", "chr1", true),
                new ContigSynonyms("chr2", "assembled-molecule", "2", GENBANK_2, REFSEQ_2, "chr2", false),
                new ContigSynonyms(SEQUENCE_NAME_3, "unlocalized-scaffold", "1", GENBANK_3, REFSEQ_3, "chr3_random",
                                   true),
                new ContigSynonyms(SEQUENCE_NAME_4, "unlocalized-scaffold", "4", GENBANK_4, REFSEQ_4, "chr4_random",
                                   true)));
        fastaSequenceReader = new FastaSynonymSequenceReader(contigMapping, fastaPath);
        accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader, contigMapping,
                                                          ContigNaming.NO_REPLACEMENT);
        executionContext = new ExecutionContext();
        accessionReportWriter.open(executionContext);
    }

    @Test
    public void writeSnpWithAccession() throws IOException {
        writeVariantWithAccessionHelper(START_1, REFERENCE, ALTERNATE, START_1, REFERENCE, ALTERNATE);
    }

    private void writeVariantWithAccessionHelper(int start, String reference, String alternate, int denormalizedStart,
                                                 String denormalizedReference, String denormalizedAlternate)
            throws IOException {
        Variant variant = buildMockVariant(CONTIG_1, CONTIG_1, start, reference, alternate);

        SubmittedVariant submittedVariant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, start,
                                                                 reference, alternate, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "1",
                                                                                                    submittedVariant);

        accessionReportWriter.write(Collections.singletonList(variant), Collections.singletonList(accessionWrapper));


        assertEquals(String.join("\t", CONTIG_1, Integer.toString(denormalizedStart), ACCESSION_PREFIX + ACCESSION,
                                 denormalizedReference, denormalizedAlternate, ".", ".", "."),
                     getFirstVariantLine(output));
    }

    public static String getFirstVariantLine(File output) throws IOException {
        BufferedReader fileInputStream = new BufferedReader(new InputStreamReader(new FileInputStream(output)));
        String line;
        while ((line = fileInputStream.readLine()) != null) {
            if (!line.startsWith("#")) {
                return line;
            }
        }
        throw new IllegalStateException("VCF didn't have any data lines");
    }

    @Test
    public void writeInsertionWithAccession() throws IOException {
        writeVariantWithAccessionHelper(START_1, "", ALTERNATE, START_1 - 1, CONTEXT_BASE, CONTEXT_BASE + ALTERNATE);
    }

    @Test
    public void writeInsertionWithAccessionFirstPositionStart() throws IOException {
        writeVariantWithAccessionHelper(1, "", "A", 1, "G", "AG");
    }

    @Test
    public void writeDeletionWithAccession() throws IOException {
        writeVariantWithAccessionHelper(START_1, REFERENCE, "", START_1 - 1, CONTEXT_BASE + REFERENCE, CONTEXT_BASE);
    }

    @Test
    public void writeDeletionWithAccessionFirstPositionStart() throws IOException {
        writeVariantWithAccessionHelper(1, "G", "", 1, "GT", "T");
    }

    @Test
    public void resumeWriting() throws IOException {
        Variant variant = buildMockVariant(CHROMOSOME_1, CONTIG_1, START_1, REFERENCE, ALTERNATE);


        SubmittedVariant submittedVariant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START_1,
                                                                 REFERENCE, ALTERNATE, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "1",
                                                                                                    submittedVariant);

        accessionReportWriter.write(Collections.singletonList(variant), Collections.singletonList(accessionWrapper));
        accessionReportWriter.close();

        AccessionReportWriter resumingWriter = new AccessionReportWriter(output, fastaSequenceReader, contigMapping,
                                                                         ContigNaming.SEQUENCE_NAME);
        variant = buildMockVariant(GENBANK_2, GENBANK_2, START_1, REFERENCE, ALTERNATE);
        submittedVariant.setContig(GENBANK_2);
        resumingWriter.open(executionContext);
        accessionReportWriter.write(Collections.singletonList(variant), Collections.singletonList(accessionWrapper));
        resumingWriter.close();

        assertHeaderIsNotWrittenTwice(output);
        assertEquals(2, FileUtils.countNonCommentLines(new FileInputStream(output)));
    }

    private Variant buildMockVariant(String originalChromosome, String replacementContig, int start, String reference,
                                     String alternate) {
        Variant variant = new Variant(replacementContig, start, start, reference, alternate);
        VariantSourceEntry sourceEntry = new VariantSourceEntry("fileId", "studyId");
        sourceEntry.addAttribute(ORIGINAL_CHROMOSOME, originalChromosome);
        variant.addSourceEntry(sourceEntry);
        return variant;
    }

    private void assertHeaderIsNotWrittenTwice(File output) throws IOException {
        BufferedReader fileInputStream = new BufferedReader(new InputStreamReader(new FileInputStream(output)));
        String line;
        do {
            line = fileInputStream.readLine();
            assertNotNull("VCF report was shorter than expected", line);
        } while (line.startsWith("#"));

        String variantLine = line;
        do {
            assertFalse("VCF report has header lines after variant lines", variantLine.startsWith("#"));
            variantLine = fileInputStream.readLine();
        } while (variantLine != null);
    }

    private List<AccessionWrapper<ISubmittedVariant, String, Long>> mockWrap(List<SubmittedVariant> variants) {
        return variants.stream()
                       .map(variant -> new AccessionWrapper<ISubmittedVariant, String, Long>(ACCESSION, HASH, variant))
                       .collect(Collectors.toList());
    }

    @Test
    public void writeChromosomeAsSubmitted() throws IOException {
        assertContigReplacement(CHROMOSOME_1, CONTIG_1, ContigNaming.NO_REPLACEMENT, CHROMOSOME_1);
    }

    private void assertContigReplacement(String originalContig, String accessionedContig,
                                         ContigNaming requestedReplacement, String replacementContig)
            throws IOException {
        Variant variant = buildMockVariant(originalContig, accessionedContig, START_1, REFERENCE, ALTERNATE);

        SubmittedVariant submittedVariant = new SubmittedVariant("accession", TAXONOMY, "project", accessionedContig,
                                                                 START_1, "", ALTERNATE, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "hash-1",
                                                                                                    submittedVariant);

        accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader, contigMapping,
                                                          requestedReplacement);
        accessionReportWriter.open(new ExecutionContext());
        accessionReportWriter.write(Collections.singletonList(variant), Collections.singletonList(accessionWrapper));

        assertEquals(replacementContig, getFirstVariantLine(output).split("\t")[CHROMOSOME_COLUMN_VCF]);
    }

    @Test
    public void writeChromosomeReplacedFromContig() throws IOException {
        assertContigReplacement(CHROMOSOME_1, CONTIG_1, ContigNaming.SEQUENCE_NAME, CHROMOSOME_1);
    }

    @Test(expected = IllegalArgumentException.class)
    public void writeContigWithoutEquivalent() throws IOException {
        String contigMissingInAssemblyReport = "contig_missing_in_assembly_report";
        assertContigReplacement(contigMissingInAssemblyReport, contigMissingInAssemblyReport, ContigNaming.SEQUENCE_NAME,
                                contigMissingInAssemblyReport);
    }

    @Test
    public void writeContigWithNoAvailableAssignedMolecule() throws IOException {
        assertContigReplacement(GENBANK_3, GENBANK_3, ContigNaming.ASSIGNED_MOLECULE, GENBANK_3);
    }

    @Test
    public void writeContigWithoutIdenticalReplacement() throws IOException {
        assertContigReplacement(REFSEQ_2, REFSEQ_2, ContigNaming.INSDC, REFSEQ_2);
    }

    /**
     * This test checks that the context base is added from the FASTA, and it doesn't matter which contig naming the
     * FASTA uses: it's independent of the accessioned contig naming (GenBank) and the VCF report contig naming.
     *
     * Note how in the fasta there's no entry for GENBANK_4 nor SEQUENCE_NAME_4, only for REFSEQ_4
     */
    @Test
    public void writeContigWithSynonymFasta() throws IOException {
        assertContigReplacement(GENBANK_4, GENBANK_4, ContigNaming.SEQUENCE_NAME, SEQUENCE_NAME_4);
    }

    @Test
    public void checkOriginalChromosomeIsWrittenInVcfInfo() throws IOException {
        String originalChromosome = SEQUENCE_NAME_3;
        String accessionedContig = GENBANK_3;

        Variant variant = buildMockVariant(originalChromosome, accessionedContig, START_1, REFERENCE, ALTERNATE);

        SubmittedVariant submittedVariant = new SubmittedVariant("accession", TAXONOMY, "project", accessionedContig,
                                                        START_1, "", ALTERNATE, CLUSTERED_VARIANT,
                                                        SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                        VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "hash-1",
                                                                                                    submittedVariant);


        accessionReportWriter.write(Collections.singletonList(variant), Collections.singletonList(accessionWrapper));


        BufferedReader fileInputStream = new BufferedReader(new InputStreamReader(new FileInputStream(output)));
        String line;
        while ((line = fileInputStream.readLine()) != null) {
            if (line.startsWith("##contig")) {
                String[] attributePairs = line.substring(line.indexOf("<"), line.length() - 1).split(",");
                assertEquals(originalChromosome, attributePairs[0].split("=")[1]);
                assertTrue(attributePairs[1].split("=")[1].contains(accessionedContig));
            }
        }
        assertEquals(originalChromosome, getFirstVariantLine(output).split("\t")[CHROMOSOME_COLUMN_VCF]);
    }
}
