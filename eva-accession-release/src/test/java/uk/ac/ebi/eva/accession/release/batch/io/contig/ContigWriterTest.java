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
package uk.ac.ebi.eva.accession.release.batch.io.contig;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.batch.item.Chunk;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.utils.PipelineTemporaryFolderUtil;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ContigWriterTest {

    private static final String EMPTY_STRING = "";

    private static final String GENBANK_ACCESSION_1 = "CM0001.1";

    private static final String GENBANK_ACCESSION_2 = "CM0002.1";

    private static final String GENBANK_ACCESSION_3 = "CM0003.1";

    private static final String GENBANK_ACCESSION_EMPTY_SEQUENCE_NAME = "CM0004.1";

    private static final String GENBANK_ACCESSION_NOT_IN_ASSEMBLY_REPORT = "CM0005.1";

    private static final String SEQUENCE_NAME_1 = "Chr1";

    private static final String SEQUENCE_NAME_2 = "Chr2";

    private static final String SEQUENCE_NAME_3 = "Chr3";

    private static final String REFSEQ_ACCESSION_1 = "NC0001.1";

    private static final String REFSEQ_ACCESSION_2 = "NC0002.1";

    private File output;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @BeforeEach
    public void setUp() throws Exception {
        output = temporaryFolderUtil.newFile();
    }

    @Test
    public void write() throws Exception {
        ContigMapping contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(SEQUENCE_NAME_1, "assembled-molecule", "1", GENBANK_ACCESSION_1, REFSEQ_ACCESSION_1,
                        "ucsc1", true),
                new ContigSynonyms(SEQUENCE_NAME_2, "assembled-molecule", "2", GENBANK_ACCESSION_2, REFSEQ_ACCESSION_2,
                        "ucsc2", false),
                new ContigSynonyms(SEQUENCE_NAME_3, "assembled-molecule", "3", GENBANK_ACCESSION_3, "na", "ucsc3",
                        false)));
        ContigWriter contigWriter = new ContigWriter(output, contigMapping);

        contigWriter.open(null);
        List<String> contigs = Arrays.asList(GENBANK_ACCESSION_1, GENBANK_ACCESSION_2, GENBANK_ACCESSION_3);
        contigWriter.write(new Chunk<>(contigs));
        contigWriter.close();

        assertEquals(contigs.size(), numberOfLines(output));

        List<String> expectedLines = Arrays.asList("CM0001.1,Chr1", "CM0002.1,Chr2", "CM0003.1,Chr3");
        assertContigFileContent(output, expectedLines);
    }

    private long numberOfLines(File file) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        return br.lines().count();
    }

    private void assertContigFileContent(File file, List<String> expectedLines) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(file));
        String line;
        while ((line = br.readLine()) != null) {
            assertTrue(expectedLines.contains(line));
        }
    }

    /**
     * The Replacement to sequence name must be performed when the contig is a RefSeq Accession
     */
    @Test
    public void useSequenceNameIfContigIsRefSeqAccession() throws IOException {
        ContigMapping contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(SEQUENCE_NAME_1, "assembled-molecule", "1", GENBANK_ACCESSION_1, REFSEQ_ACCESSION_1,
                        "ucsc1", true),
                new ContigSynonyms(SEQUENCE_NAME_2, "assembled-molecule", "2", GENBANK_ACCESSION_2, REFSEQ_ACCESSION_2,
                        "ucsc2", false),
                new ContigSynonyms(SEQUENCE_NAME_3, "assembled-molecule", "3", GENBANK_ACCESSION_3, "na", "ucsc3",
                        false)));
        ContigWriter contigWriter = new ContigWriter(output, contigMapping);

        contigWriter.open(null);
        List<String> contigs = Arrays.asList(REFSEQ_ACCESSION_1, REFSEQ_ACCESSION_2, GENBANK_ACCESSION_3);
        contigWriter.write(new Chunk<>(contigs));
        contigWriter.close();

        assertEquals(contigs.size(), numberOfLines(output));

        List<String> expectedLines = Arrays.asList("NC0001.1,Chr1", "NC0002.1,Chr2", "CM0003.1,Chr3");
        assertContigFileContent(output, expectedLines);
    }

    /**
     * This will happen a contig is null
     */
    @Test
    public void throwExceptionIfNullContig() {
        ContigMapping contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(SEQUENCE_NAME_1, "assembled-molecule", "1", GENBANK_ACCESSION_1, REFSEQ_ACCESSION_1,
                        "ucsc1", true)));
        ContigWriter contigWriter = new ContigWriter(output, contigMapping);

        contigWriter.open(null);
        String nullGenbankAccession = null;
        assertThrows(IllegalArgumentException.class, () -> contigWriter.write(Chunk.of(GENBANK_ACCESSION_1, nullGenbankAccession)));
        contigWriter.close();
    }

    /**
     * This will happen a contig is empty
     */
    @Test
    public void throwExceptionIfEmptyContig() {
        ContigMapping contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(SEQUENCE_NAME_1, "assembled-molecule", "1", GENBANK_ACCESSION_1, REFSEQ_ACCESSION_1,
                        "ucsc1", true)));
        ContigWriter contigWriter = new ContigWriter(output, contigMapping);

        contigWriter.open(null);
        String emptyGenbankAccession = "";
        assertThrows(IllegalArgumentException.class, () -> contigWriter.write(Chunk.of(GENBANK_ACCESSION_1, emptyGenbankAccession)));
        contigWriter.close();
    }

    /**
     * This will happen if the assembly report does not have a value for sequence name in at least one row
     */
    @Test
    public void throwExceptionIfEmptySequenceName() {
        ContigMapping contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(SEQUENCE_NAME_1, "assembled-molecule", "1", GENBANK_ACCESSION_1, REFSEQ_ACCESSION_1,
                        "ucsc1", true),
                new ContigSynonyms(EMPTY_STRING, "assembled-molecule", "4", GENBANK_ACCESSION_EMPTY_SEQUENCE_NAME, "na",
                        "ucsc4", false)));
        ContigWriter contigWriter = new ContigWriter(output, contigMapping);

        contigWriter.open(null);
        assertThrows(IllegalArgumentException.class, () -> contigWriter.write(Chunk.of(GENBANK_ACCESSION_1, GENBANK_ACCESSION_EMPTY_SEQUENCE_NAME)));
        contigWriter.close();
    }

    /**
     * This will happen if there is any contig in mongo that is not in the assembly report
     */
    @Test
    public void throwExceptionIfContigNotInAssemblyReport() {
        ContigMapping contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(SEQUENCE_NAME_1, "assembled-molecule", "1", GENBANK_ACCESSION_1, REFSEQ_ACCESSION_1,
                        "ucsc1", true)));
        ContigWriter contigWriter = new ContigWriter(output, contigMapping);

        contigWriter.open(null);
        assertThrows(IllegalArgumentException.class, () -> contigWriter.write(Chunk.of(GENBANK_ACCESSION_1, GENBANK_ACCESSION_NOT_IN_ASSEMBLY_REPORT)));
        contigWriter.close();
    }
}