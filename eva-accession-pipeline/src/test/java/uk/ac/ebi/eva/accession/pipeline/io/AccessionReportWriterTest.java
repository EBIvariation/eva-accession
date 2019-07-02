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
import uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck.AccessionWrapperComparator;
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
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

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
                                                          ContigNaming.INSDC);
        executionContext = new ExecutionContext();
        accessionReportWriter.open(executionContext);
    }

    @Test
    public void writeSnpWithAccession() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CHROMOSOME_1, START_1, REFERENCE,
                                                        ALTERNATE, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper =
                new AccessionWrapper<ISubmittedVariant, String, Long>(ACCESSION, "1", variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);

        assertEquals(
                String.join("\t", CONTIG_1, Integer.toString(START_1), ACCESSION_PREFIX + ACCESSION, REFERENCE,
                            ALTERNATE, ".", ".", "."),
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
        writeIndelWithAccessionHelper("", ALTERNATE, CONTEXT_BASE, CONTEXT_BASE + ALTERNATE);
    }

    @Test
    public void writeInsertionWithAccessionFirstPositionStart() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CHROMOSOME_1, 1, "",
                                                        "A", CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper =
                new AccessionWrapper<ISubmittedVariant, String, Long>(ACCESSION, "1", variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);

        assertEquals(
                String.join("\t", CONTIG_1, "1", ACCESSION_PREFIX + ACCESSION, "G", "AG", ".", ".", "."),
                getFirstVariantLine(output));
    }

    private void writeIndelWithAccessionHelper(String reference, String alternate, String denormalizedReference,
                                               String denormalizedAlternate) throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CHROMOSOME_1, START_1, reference,
                                                        alternate, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "1",
                                                                                                    variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);

        assertEquals(String.join("\t", CONTIG_1, Integer.toString(START_1 - 1), ACCESSION_PREFIX + ACCESSION,
                                 denormalizedReference, denormalizedAlternate,
                                 ".", ".", "."),
                     getFirstVariantLine(output));
    }

    @Test
    public void writeDeletionWithAccession() throws IOException {
        writeIndelWithAccessionHelper(REFERENCE, "", CONTEXT_BASE + REFERENCE, CONTEXT_BASE);
    }

    @Test
    public void writeDeletionWithAccessionFirstPositionStart() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CHROMOSOME_1, 1, "G",
                                                        "", CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper =
                new AccessionWrapper<ISubmittedVariant, String, Long>(ACCESSION, "1", variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);

        assertEquals(
                String.join("\t", CONTIG_1, "1", ACCESSION_PREFIX + ACCESSION, "GT", "T", ".", ".", "."),
                getFirstVariantLine(output));
    }

    @Test
    public void resumeWriting() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START_1, REFERENCE,
                                                        ALTERNATE, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper =
                new AccessionWrapper<ISubmittedVariant, String, Long>(ACCESSION, "1", variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);
        accessionReportWriter.close();

        AccessionReportWriter resumingWriter = new AccessionReportWriter(output, fastaSequenceReader, contigMapping,
                                                                         ContigNaming.SEQUENCE_NAME);
        variant.setContig(CONTIG_2);
        resumingWriter.open(executionContext);
        resumingWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);
        resumingWriter.close();

        assertHeaderIsNotWrittenTwice(output);
        assertEquals(2, FileUtils.countNonCommentLines(new FileInputStream(output)));
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
    public void writeChromosomeReplacedFromContig() throws IOException {
        assertContigReplacement(CONTIG_1, ContigNaming.SEQUENCE_NAME, CHROMOSOME_1);
    }

    private void assertContigReplacement(String originalContig, ContigNaming requestedReplacement,
                                         String replacementContig) throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", originalContig,
                                                        START_1, "", ALTERNATE, CLUSTERED_VARIANT,
                                                        SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                        VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "hash-1",
                                                                                                    variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader, contigMapping,
                                                          requestedReplacement);
        accessionReportWriter.open(new ExecutionContext());
        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);

        assertEquals(replacementContig, getFirstVariantLine(output).split("\t")[CHROMOSOME_COLUMN_VCF]);
    }

    @Test(expected = IllegalArgumentException.class)
    public void writeContigWithoutEquivalent() throws IOException {
        String contigMissingInAssemblyReport = "contig_missing_in_assembly_report";
        assertContigReplacement(contigMissingInAssemblyReport, ContigNaming.SEQUENCE_NAME,
                                contigMissingInAssemblyReport);
    }

    @Test
    public void writeContigWithNoAvailableAssignedMolecule() throws IOException {
        assertContigReplacement(GENBANK_3, ContigNaming.ASSIGNED_MOLECULE, GENBANK_3);
    }

    @Test
    public void writeContigWithoutIdenticalReplacement() throws IOException {
        assertContigReplacement(REFSEQ_2, ContigNaming.INSDC, REFSEQ_2);
    }

    /**
     * This test checks that the context base is added from the FASTA, and it doesn't matter which contig naming the
     * FASTA uses: it's independent of the accessioned contig naming (GenBank) and the VCF report contig naming.
     *
     * Note how in the fasta there's no entry for GENBANK_4 nor SEQUENCE_NAME_4, only for REFSEQ_4
     */
    @Test
    public void writeContigWithSynonymFasta() throws IOException {
        assertContigReplacement(GENBANK_4, ContigNaming.SEQUENCE_NAME, SEQUENCE_NAME_4);
    }

    @Test
    public void checkOriginalChromosomeIsWrittenInVcfInfo() throws IOException {
        String originalChromosome = SEQUENCE_NAME_3;
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", originalChromosome,
                                                        START_1, "", ALTERNATE, CLUSTERED_VARIANT,
                                                        SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                        VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "hash-1",
                                                                                                    variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);

        assertEquals(GENBANK_3, getFirstVariantLine(output).split("\t")[CHROMOSOME_COLUMN_VCF]);
        String info = getFirstVariantLine(output).split("\t")[INFO_COLUMN_VCF];
        assertEquals(originalChromosome, info.split("=")[1]);
    }
}
