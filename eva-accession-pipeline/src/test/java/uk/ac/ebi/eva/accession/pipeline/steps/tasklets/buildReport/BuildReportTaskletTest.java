/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.pipeline.steps.tasklets.buildReport;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import uk.ac.ebi.eva.accession.pipeline.io.AccessionReportWriter;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.util.Arrays;

import static org.junit.Assert.*;

public class BuildReportTaskletTest {

    private static final String SEQUENCE_NAME_3 = "ctg3";

    private static final String GENBANK_3 = "genbank_3";

    private static final Integer START_1 = 10;

    private static final Long ACCESSION = 100L;

    private static final String REFERENCE = "T";

    private static final String ALTERNATE = "A";

    private static final String MISSING = ".";

    private static final int CHROMOSOME_COLUMN_VCF = 0;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    private File output;

    private File variantsOutput;

    private File contigsOutput;

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        variantsOutput = new File(output.getAbsolutePath() + AccessionReportWriter.VARIANTS_FILE_SUFFIX);
        contigsOutput = new File(output.getAbsolutePath() + AccessionReportWriter.CONTIGS_FILE_SUFFIX);
    }

    @Test
    public void basicChecks() throws Exception {
        writeSingleVariantInContigAndVariantFiles(SEQUENCE_NAME_3, GENBANK_3);

        BuildReportTasklet buildReportTasklet = new BuildReportTasklet(output);
        buildReportTasklet.execute(null, null);

        assertFalse(variantsOutput.exists());
        assertFalse(contigsOutput.exists());
        assertTrue(output.exists());
        assertHeaderIsNotWrittenTwice(output);

        int expectedLineCountWithOneContigAndOneVariant = 4;
        assertEquals(expectedLineCountWithOneContigAndOneVariant, Files.lines(output.toPath()).count());
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

    @Test
    public void checkOriginalChromosomeIsWrittenInVcfInfo() throws Exception {
        String originalChromosome = SEQUENCE_NAME_3;
        String accessionedContig = GENBANK_3;
        writeSingleVariantInContigAndVariantFiles(originalChromosome, accessionedContig);

        BuildReportTasklet buildReportTasklet = new BuildReportTasklet(output);
        buildReportTasklet.execute(null, null);


        BufferedReader fileInputStream = new BufferedReader(new FileReader(output));
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

    private void writeSingleVariantInContigAndVariantFiles(String originalChromosome, String accessionedContig) throws IOException {
        FileWriter contigsWriter = new FileWriter(contigsOutput);
        contigsWriter.write(String.join("\t", Arrays.asList(originalChromosome, accessionedContig)));
        contigsWriter.close();

        FileWriter variantsWriter = new FileWriter(variantsOutput);
        variantsWriter.write(String.join("\t",
                                         Arrays.asList(originalChromosome, START_1.toString(), ACCESSION.toString(),
                                                       REFERENCE, ALTERNATE, MISSING, MISSING, MISSING)));
        variantsWriter.close();
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
}