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

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

public class AccessionReportWriterTest {

    private static final String CONTIG_1 = "contig_1";

    private static final String CONTIG_2 = "contig_2";

    private static final int START = 10;

    private static final String REFERENCE = "T";

    private static final String ALTERNATE = "A";

    private static final String CONTEXT_BASE = "G";

    private static final int TAXONOMY = 3880;

    private static final String ACCESSION_PREFIX = "ss";

    private static final long ACCESSION = 100L;

    private AccessionReportWriter accessionReportWriter;

    private File output;

    private FastaSequenceReader fastaSequenceReader;

    private ExecutionContext executionContext;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        Path fastaPath = Paths.get(AccessionReportWriterTest.class.getResource("/input-files/fasta/mock.fa").getFile());
        fastaSequenceReader = new FastaSequenceReader(fastaPath);
        accessionReportWriter = new AccessionReportWriter(output, fastaSequenceReader);
        executionContext = new ExecutionContext();
        accessionReportWriter.open(executionContext);
    }

    @Test
    public void writeSnpWithAccession() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START, REFERENCE,
                                                        ALTERNATE, false);

        accessionReportWriter.write(Collections.singletonMap(ACCESSION, variant));

        assertEquals(String.join("\t", CONTIG_1, Integer.toString(START), ACCESSION_PREFIX + ACCESSION,
                                 REFERENCE, ALTERNATE, ".", ".", "."),
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
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START, "",
                                                        ALTERNATE, false);

        accessionReportWriter.write(Collections.singletonMap(ACCESSION, variant));

        assertEquals(String.join("\t", CONTIG_1, Integer.toString(START - 1), ACCESSION_PREFIX + ACCESSION,
                                 CONTEXT_BASE, CONTEXT_BASE + ALTERNATE,
                                 ".", ".", "."),
                     getFirstVariantLine(output));
    }

    @Test
    public void writeDeletionWithAccession() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START, REFERENCE,
                                                        "", false);

        accessionReportWriter.write(Collections.singletonMap(ACCESSION, variant));

        assertEquals(String.join("\t", CONTIG_1, Integer.toString(START - 1), ACCESSION_PREFIX + ACCESSION,
                                 CONTEXT_BASE + REFERENCE, CONTEXT_BASE,
                                 ".", ".", "."),
                     getFirstVariantLine(output));
    }

    @Test
    public void resumeWriting() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG_1, START, REFERENCE,
                                                        ALTERNATE, false);

        accessionReportWriter.write(Collections.singletonMap(ACCESSION, variant));
        accessionReportWriter.close();

        AccessionReportWriter resumingWriter = new AccessionReportWriter(output, fastaSequenceReader);
        variant.setContig(CONTIG_2);
        resumingWriter.open(executionContext);
        resumingWriter.write(Collections.singletonMap(ACCESSION, variant));
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
}
