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
import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck.AccessionWrapperComparator;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class AccessionReportWriterTest {

    private static final String CONTIG_1 = "contig_1";

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

    private AccessionReportWriter accessionReportWriter;

    private File output;

    private FastaSequenceReader fastaSequenceReader;

    private ExecutionContext executionContext;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        Path fastaPath = Paths.get(AccessionReportWriterTest.class.getResource("/input-files/fasta/mock.fa").toURI());
        fastaSequenceReader = new FastaSequenceReader(fastaPath);
        accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader);
        executionContext = new ExecutionContext();
        accessionReportWriter.open(executionContext);
    }

    @Test
    public void writeSnpWithAccession() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START_1, REFERENCE,
                                                        ALTERNATE, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED);

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

    private void writeIndelWithAccessionHelper(String reference, String alternate, String denormalizedReference,
                                               String denormalizedAlternate) throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START_1, reference,
                                                        alternate, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED);

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
    public void resumeWriting() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START_1, REFERENCE,
                                                        ALTERNATE, CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED);

        AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper =
                new AccessionWrapper<ISubmittedVariant, String, Long>(ACCESSION, "1", variant);

        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator(
                Collections.singletonList(variant));

        accessionReportWriter.write(Collections.singletonList(accessionWrapper), accessionWrapperComparator);
        accessionReportWriter.close();

        AccessionReportWriter resumingWriter = new AccessionReportWriter(output, fastaSequenceReader);
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
}
