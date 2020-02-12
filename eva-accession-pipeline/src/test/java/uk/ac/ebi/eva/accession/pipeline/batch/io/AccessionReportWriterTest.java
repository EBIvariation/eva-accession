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
package uk.ac.ebi.eva.accession.pipeline.batch.io;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.service.GetOrCreateAccessionWrapperCreator;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigNaming;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.batch.processors.ContigToGenbankReplacerProcessor.ORIGINAL_CHROMOSOME;

public class AccessionReportWriterTest {

    private static final String CONTIG_1 = "genbank_1";

    private static final String CHROMOSOME_1 = "chr1";

    private static final int START_1 = 10;

    private static final int START_2 = 14;

    private static final String REFERENCE = "T";

    private static final String REFERENCE_1 = "G";

    private static final String ALTERNATE = "A";

    private static final String CONTEXT_BASE = "G";

    private static final String CONTEXT_BASE_1 = "T";

    private static final int TAXONOMY = 3880;

    private static final String ACCESSION_PREFIX = "ss";

    private static final long ACCESSION = 100L;

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = null;

    private static final Boolean MATCHES_ASSEMBLY = null;

    private static final Boolean ALLELES_MATCH = null;

    private static final Boolean VALIDATED = null;

    private static final int CHROMOSOME_COLUMN_VCF = 0;

    private static final String GENBANK_2 = "genbank_2";

    private static final String REFSEQ_2 = "refseq_2";

    private static final String SEQUENCE_NAME_3 = "ctg3";

    private static final String GENBANK_3 = "genbank_3";

    private static final String REFSEQ_3 = "refseq_3";

    private static final String SEQUENCE_NAME_4 = "ctg4";

    private static final String GENBANK_4 = "genbank_4";

    private static final String REFSEQ_4 = "refseq_4";

    private File output;

    private File variantsOutput;

    private File contigsOutput;

    private FastaSequenceReader fastaSequenceReader;

    private ExecutionContext executionContext;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    private ContigMapping contigMapping;

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        variantsOutput = new File(output.getAbsolutePath() + AccessionReportWriter.VARIANTS_FILE_SUFFIX);
        contigsOutput = new File(output.getAbsolutePath() + AccessionReportWriter.CONTIGS_FILE_SUFFIX);
        Path fastaPath = Paths.get(AccessionReportWriterTest.class.getResource("/input-files/fasta/mock.fa").toURI());
        contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(CHROMOSOME_1, "assembled-molecule", "1", CONTIG_1, "refseq_1", "chr1", true),
                new ContigSynonyms("chr2", "assembled-molecule", "2", GENBANK_2, REFSEQ_2, "chr2", false),
                new ContigSynonyms(SEQUENCE_NAME_3, "unlocalized-scaffold", "1", GENBANK_3, REFSEQ_3, "chr3_random",
                                   true),
                new ContigSynonyms(SEQUENCE_NAME_4, "unlocalized-scaffold", "4", GENBANK_4, REFSEQ_4, "chr4_random",
                                   true)));
        fastaSequenceReader = new FastaSynonymSequenceReader(contigMapping, fastaPath);
        executionContext = new ExecutionContext();
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

        AccessionReportWriter accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader,
                                                                                contigMapping,
                                                                                ContigNaming.NO_REPLACEMENT);
        accessionReportWriter.open(executionContext);
        accessionReportWriter.write(Collections.singletonList(variant),
                                    GetOrCreateAccessionWrapperCreator.convertToGetOrCreateAccessionWrapper(
                                            Collections.singletonList(accessionWrapper)));


        assertEquals(String.join("\t", CONTIG_1, Integer.toString(denormalizedStart), ACCESSION_PREFIX + ACCESSION,
                                 denormalizedReference, denormalizedAlternate, ".", ".", "."),
                     getFirstVariantLine(variantsOutput));
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
        writeVariantWithAccessionHelper(1, "", ALTERNATE, 1, CONTEXT_BASE, ALTERNATE + CONTEXT_BASE);
    }

    @Test
    public void writeDeletionWithAccession() throws IOException {
        writeVariantWithAccessionHelper(START_1, REFERENCE, "", START_1 - 1, CONTEXT_BASE + REFERENCE, CONTEXT_BASE);
    }

    @Test
    public void writeDeletionWithAccessionFirstPositionStart() throws IOException {
        writeVariantWithAccessionHelper(1, REFERENCE_1, "", 1, REFERENCE_1 + CONTEXT_BASE_1, CONTEXT_BASE_1);
    }

    @Test
    public void resumeWriting() throws IOException {
        writeAndResumeAndWrite(CHROMOSOME_1, CONTIG_1, START_1, GENBANK_2, GENBANK_2, START_1);

        assertEquals(2, Files.lines(contigsOutput.toPath()).count());
        assertEquals(2, Files.lines(variantsOutput.toPath()).count());
    }

    private void writeAndResumeAndWrite(String originalChromosome1, String contig1, int start1, String originalChromosome2,
                                        String contig2, int start2) throws IOException {
        // first writer
        Variant variant = buildMockVariant(originalChromosome1, contig1, start1, REFERENCE, ALTERNATE);


        SubmittedVariant submittedVariant = new SubmittedVariant("accession", TAXONOMY, "project", contig1, start1,
                                                                 REFERENCE, ALTERNATE, CLUSTERED_VARIANT,
                                                                 SUPPORTED_BY_EVIDENCE, MATCHES_ASSEMBLY, ALLELES_MATCH,
                                                                 VALIDATED, null);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper = new AccessionWrapper<>(ACCESSION, "1",
                                                                                                    submittedVariant);

        AccessionReportWriter accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader,
                                                                                contigMapping,
                                                                                ContigNaming.NO_REPLACEMENT);
        accessionReportWriter.open(executionContext);
        accessionReportWriter.write(Collections.singletonList(variant),
                                    GetOrCreateAccessionWrapperCreator.convertToGetOrCreateAccessionWrapper(
                                            Collections.singletonList(accessionWrapper)));
        accessionReportWriter.close();

        // second writer
        AccessionReportWriter resumingWriter = new AccessionReportWriter(output, fastaSequenceReader, contigMapping,
                                                                         ContigNaming.NO_REPLACEMENT);
        variant = buildMockVariant(originalChromosome2, contig2, start2, REFERENCE, ALTERNATE);
        submittedVariant.setContig(contig2);
        submittedVariant.setStart(start2);

        resumingWriter.open(executionContext);
        resumingWriter.write(Collections.singletonList(variant),
                             GetOrCreateAccessionWrapperCreator.convertToGetOrCreateAccessionWrapper(
                                     Collections.singletonList(accessionWrapper)));
        resumingWriter.close();
    }

    @Test
    public void resumeWritingWithSameContigs() throws IOException {
        writeAndResumeAndWrite(CHROMOSOME_1, CONTIG_1, START_1, CHROMOSOME_1, CONTIG_1, START_2);

        assertEquals(1, Files.lines(contigsOutput.toPath()).count());
        assertEquals(2, Files.lines(variantsOutput.toPath()).count());
    }

    @Test
    @Ignore("A duplicated variant (two input variants that get the same accession) will appear twice in the VCF "
            + "report if they are processed in different batches. There is no easy solution for this, as we would "
            + "need to cache previous batches. It is even worse if the job fails between the affected batches, as we "
            + "would need to parse the temporary VCF output or keep some variants in the job repository.")
    public void resumeWritingWithRepeatedVariant() throws IOException {
        writeAndResumeAndWrite(CHROMOSOME_1, CONTIG_1, START_1, CHROMOSOME_1, CONTIG_1, START_1);

        assertEquals(1, Files.lines(contigsOutput.toPath()).count());
        assertEquals(1, Files.lines(variantsOutput.toPath()).count());
    }

    private Variant buildMockVariant(String originalChromosome, String replacementContig, int start, String reference,
                                     String alternate) {
        Variant variant = new Variant(replacementContig, start, start, reference, alternate);
        VariantSourceEntry sourceEntry = new VariantSourceEntry("fileId", "studyId");
        sourceEntry.addAttribute(ORIGINAL_CHROMOSOME, originalChromosome);
        variant.addSourceEntry(sourceEntry);
        return variant;
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

        AccessionReportWriter accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader,
                                                                                contigMapping, requestedReplacement);
        accessionReportWriter.open(new ExecutionContext());
        accessionReportWriter.write(Collections.singletonList(variant),
                                    GetOrCreateAccessionWrapperCreator.convertToGetOrCreateAccessionWrapper(
                                            Collections.singletonList(accessionWrapper)));

        assertEquals(replacementContig, getFirstVariantLine(variantsOutput).split("\t")[CHROMOSOME_COLUMN_VCF]);
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

}
