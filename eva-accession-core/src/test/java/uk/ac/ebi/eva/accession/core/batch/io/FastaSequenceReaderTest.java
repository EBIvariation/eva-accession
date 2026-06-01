/*
 * Copyright 2017 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.batch.io;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import uk.ac.ebi.eva.accession.core.utils.PipelineTemporaryFolderUtil;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FastaSequenceReaderTest {

    private static final String MIXED_CASE_FASTA_CONTIG = "NW_006738765.1";

    private FastaSequenceReader reader;

    public PipelineTemporaryFolderUtil temporaryFolderUtil = new PipelineTemporaryFolderUtil();

    @BeforeEach
    public void setUp() throws Exception {
        reader = new FastaSequenceReader(Paths.get(
                FastaSequenceReaderTest.class.getResource("/input-files/fasta/Gallus_gallus-5.0.test.fa").toURI()));
    }

    @AfterEach
    public void tearDown() throws Exception {
        reader.close();
    }

    @Test
    public void getFirstNucleotideOfContig() {
        assertEquals("T", reader.getSequence("22", 1, 1));
    }

    @Test
    public void getLastNucleotideOfContig() {
        assertEquals("G", reader.getSequence("22", 4729743, 4729743));
    }

    @Test
    public void getSequence() {
        // this sequence is split between three lines in the FASTA file
        assertEquals("GTTTCAAGTGGTTGTGACCCCCGCTGCACAGTCAGTTGGGTTAGGGTTAGGGTTAGGGTCAGTCACAGTCAGTTGTCAGACTGGTGTTTA",
                reader.getSequence("22", 59986, 60075));
    }

    @Test
    public void endMustBeGreaterOrEqualsThanStart() {
        assertThrows(IllegalArgumentException.class, () -> reader.getSequence("22", 1000, 999));
    }

    @Test
    public void onlyPositiveCoordinatesAreAllowed() {
        assertThrows(IllegalArgumentException.class, () -> reader.getSequence("22", -1, 5));
    }

    @Test
    public void coordinatesGreaterThanEndOfChromosomeAreNotAllowed() {
        assertThrows(IllegalArgumentException.class, () -> reader.getSequence("22", 4729740, 4729750));
    }

    @Test
    public void notExistentChromosome() {
        assertThrows(IllegalArgumentException.class, () -> reader.getSequence("23", 1, 1));
    }

    @Test
    public void fastaWithNoDictionary() throws Exception {
        FastaSequenceReader fastaSequenceReader = new FastaSequenceReader(Paths.get(
                FastaSequenceReaderTest.class.getResource("/input-files/fasta/fastaWithNoDictionary.fa").toURI()));
        // this sequence is split between three lines in the FASTA file
        assertEquals("CAGCCGCAGTCCGGACAGCGCATGCGCCAGCCGCGAGACCGCACAGCGCATGCGCCAGCGCGAGTGACAGCG",
                fastaSequenceReader.getSequence("22", 174, 245));
    }

    @Test
    public void fastaWithNoIndex() throws Exception {
        String fastaFilename = "fastaWithNoDictionary.fa";
        FastaSequenceReader fastaSequenceReader = getFastaSequenceReader(fastaFilename);

        // this sequence is split between three lines in the FASTA file
        assertEquals("CAGCCGCAGTCCGGACAGCGCATGCGCCAGCCGCGAGACCGCACAGCGCATGCGCCAGCGCGAGTGACAGCG",
                fastaSequenceReader.getSequence("22", 174, 245));
    }

    private FastaSequenceReader getFastaSequenceReader(String fastaFilename) throws IOException, URISyntaxException {
        File temporaryFolderRoot = temporaryFolderUtil.getRoot();
        Path fasta = Files.copy(
                Paths.get(FastaSequenceReaderTest.class.getResource("/input-files/fasta/" + fastaFilename).toURI()),
                temporaryFolderRoot.toPath().resolve(fastaFilename));

        return new FastaSequenceReader(fasta);
    }

    /**
     * @TODO find the bug: either we don't use properly htsjdk, or they have a bug reading compressed fastas.
     * This test is ignored because to run it we have to remove the requirement in
     * {@link FastaSequenceReader#checkFastaIsUncompressed(java.nio.file.Path)} that
     * forbids compressed fastas. You can comment the requirement by hand and run this test to see if it still applies.
     */
    @Test
    @Disabled
    public void htsDoesNotSupportCompressedFastas() throws URISyntaxException, IOException {
        String fastaFilename = "compressed.fa.gz";
        FastaSequenceReader fastaSequenceReader = getFastaSequenceReader(fastaFilename);

        assertEquals("\0\0\0\0", fastaSequenceReader.getSequence("22", 174, 177));
    }

    /**
     * For the rationale of this test, look at {@link #htsDoesNotSupportCompressedFastas()} and
     * {@link FastaSequenceReader#checkFastaIsUncompressed(java.nio.file.Path)}
     */
    @Test
    public void shouldThrowOnCompressedFasta() throws URISyntaxException, IOException {
        String fastaFilename = "compressed.fa.gz";
        File temporaryFolderRoot = temporaryFolderUtil.getRoot();
        Path fasta = Files.copy(
                Paths.get(FastaSequenceReaderTest.class.getResource("/input-files/fasta/" + fastaFilename).toURI()),
                temporaryFolderRoot.toPath().resolve(fastaFilename));

        assertThrows(IllegalArgumentException.class, () -> new FastaSequenceReader(fasta));
    }

    @Test
    public void shouldConvertToUpper() throws IOException, URISyntaxException {
        FastaSequenceReader fastaSequenceReader = getFastaSequenceReader("fastaWithSoftMasking.fa");

        assertEquals("g", fastaSequenceReader.getSequence(MIXED_CASE_FASTA_CONTIG, 1, 1));
        assertEquals("G", fastaSequenceReader.getSequenceToUpperCase(MIXED_CASE_FASTA_CONTIG, 1, 1));
    }

    @Test
    public void getContextNucleotideAndNewStart() {
        ImmutableTriple expected = new ImmutableTriple<Long, String, String>(1L, "T", "AT");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 1, "", "A"));

        expected = new ImmutableTriple<Long, String, String>(1L, "T", "TA");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 2, "", "A"));

        expected = new ImmutableTriple<Long, String, String>(1L, "T", "CAT");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 1, "", "CA"));

        expected = new ImmutableTriple<Long, String, String>(1L, "T", "TCA");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 2, "", "CA"));

        expected = new ImmutableTriple<Long, String, String>(1L, "TG", "T");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 2, "G", ""));

        expected = new ImmutableTriple<Long, String, String>(1L, "TGC", "T");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 2, "GC", ""));

        expected = new ImmutableTriple<Long, String, String>(1L, "TG", "G");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 1, "T", ""));

        expected = new ImmutableTriple<Long, String, String>(1L, "TGC", "C");
        assertEquals(expected, reader.getContextNucleotideAndNewStart("22", 1, "TG", ""));
    }
}
