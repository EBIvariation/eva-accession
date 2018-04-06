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

import uk.ac.ebi.eva.accession.core.SubmittedVariant;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

public class AccessionSummaryWriterTest {

    private static final String CONTIG = "contig";

    private static final int START = 10;

    private static final String REFERENCE = "T";

    private static final String ALTERNATE = "A";

    private static final String CONTEXT_BASE = "G";

    private static final int TAXONOMY = 3880;

    private AccessionSummaryWriter accessionWriter;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    private File output;

    private static final long RS_ID = 100L;

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        Path fastaPath = Paths.get(AccessionSummaryWriterTest.class.getResource("/input-files/fasta/mock.fa").getFile());
        accessionWriter = new AccessionSummaryWriter(output, new FastaSequenceReader(fastaPath));
    }

    @Test
    public void writeSnpWithAccession() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG, START, REFERENCE,
                                                        ALTERNATE, false);

        accessionWriter.write(Collections.singletonMap(RS_ID, variant));

        assertEquals(String.join("\t", CONTIG, Integer.toString(START), "rs" + RS_ID, REFERENCE, ALTERNATE, ".", ".", "."),
                     getFirstVariantLine());
    }

    private String getFirstVariantLine() throws IOException {
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
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG, START, "",
                                                        ALTERNATE, false);

        accessionWriter.write(Collections.singletonMap(RS_ID, variant));

        assertEquals(String.join("\t", CONTIG, Integer.toString(START - 1), "rs" + RS_ID,
                                 CONTEXT_BASE, CONTEXT_BASE + ALTERNATE,
                                 ".", ".", "."),
                     getFirstVariantLine());
    }

    @Test
    public void writeDeletionWithAccession() throws IOException {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", CONTIG, START, REFERENCE,
                                                        "", false);

        accessionWriter.write(Collections.singletonMap(RS_ID, variant));

        assertEquals(String.join("\t", CONTIG, Integer.toString(START - 1), "rs" + RS_ID,
                                 CONTEXT_BASE + REFERENCE, CONTEXT_BASE,
                                 ".", ".", "."),
                     getFirstVariantLine());
    }
}
