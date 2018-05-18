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
package uk.ac.ebi.eva.accession.pipeline.io;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.Assert.assertEquals;

public class FastaSequenceReaderTest {

    private FastaSequenceReader reader;

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setUp() throws Exception {
        reader = new FastaSequenceReader(Paths.get(
                FastaSequenceReaderTest.class.getResource("/input-files/fasta/Gallus_gallus-5.0.test.fa").getFile()));
    }

    @After
    public void tearDown() throws Exception {
        reader.close();
    }

    @Test
    public void getFirstNucleotideOfContig() throws Exception {
        assertEquals("T", reader.getSequence("22", 1, 1));
    }

    @Test
    public void getLastNucleotideOfContig() throws Exception {
        assertEquals("G", reader.getSequence("22", 4729743, 4729743));
    }

    @Test
    public void getSequence() throws Exception {
        // this sequence is split between three lines in the FASTA file
        assertEquals("GTTTCAAGTGGTTGTGACCCCCGCTGCACAGTCAGTTGGGTTAGGGTTAGGGTTAGGGTCAGTCACAGTCAGTTGTCAGACTGGTGTTTA",
                     reader.getSequence("22", 59986, 60075));
    }

    @Test
    public void endMustBeGreaterOrEqualsThanStart() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        reader.getSequence("22", 1000, 999);
    }

    @Test
    public void onlyPositiveCoordinatesAreAllowed() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        reader.getSequence("22", -1, 5);
    }

    @Test
    public void coordinatesGreaterThanEndOfChromosomeAreNotAllowed() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        reader.getSequence("22", 4729740, 4729750);
    }

    @Test
    public void notExistentChromosome() throws Exception {
        thrown.expect(IllegalArgumentException.class);
        reader.getSequence("23", 1, 1);
    }

    @Test
    public void fastaWithNoDictionary() throws Exception {
        FastaSequenceReader fastaSequenceReader = new FastaSequenceReader(Paths.get(
                FastaSequenceReaderTest.class.getResource("/input-files/fasta/fastaWithNoDictionary.fa").getFile()));
        // this sequence is split between three lines in the FASTA file
        assertEquals("CAGCCGCAGTCCGGACAGCGCATGCGCCAGCCGCGAGACCGCACAGCGCATGCGCCAGCGCGAGTGACAGCG",
                     fastaSequenceReader.getSequence("22", 174, 245));
    }

    @Test
    public void fastaWithNoIndex() throws Exception {
        String fastaFilename = "fastaWithNoDictionary.fa";
        File temporaryFolderRoot = temporaryFolder.getRoot();
        Path fasta = Files.copy(
                Paths.get(FastaSequenceReaderTest.class.getResource("/input-files/fasta/" + fastaFilename).getFile()),
                temporaryFolderRoot.toPath().resolve(fastaFilename));

        FastaSequenceReader fastaSequenceReader = new FastaSequenceReader(fasta);

        // this sequence is split between three lines in the FASTA file
        assertEquals("CAGCCGCAGTCCGGACAGCGCATGCGCCAGCCGCGAGACCGCACAGCGCATGCGCCAGCGCGAGTGACAGCG",
                     fastaSequenceReader.getSequence("22", 174, 245));
    }

    /**
     * @TODO find the bug: either we don't use properly htsjdk, or they have a bug reading compressed fastas.
     * This test is ignored because to run it we have to remove the requirement in
     * {@link uk.ac.ebi.eva.accession.pipeline.io.FastaSequenceReader#checkFastaIsUncompressed(java.nio.file.Path)} that
     * forbids compressed fastas. You can comment the requirement by hand and run this test to see if it still applies.
     */
    @Test
    @Ignore
    public void htsDoesNotSupportCompressedFastas() throws URISyntaxException, IOException {
        String fastaFilename = "compressed.fa.gz";
        File temporaryFolderRoot = temporaryFolder.getRoot();
        Path fasta = Files.copy(
                Paths.get(FastaSequenceReaderTest.class.getResource("/input-files/fasta/" + fastaFilename).toURI()),
                temporaryFolderRoot.toPath().resolve(fastaFilename));

        FastaSequenceReader fastaSequenceReader = new FastaSequenceReader(fasta);

        assertEquals("\0\0\0\0", fastaSequenceReader.getSequence("22", 174, 177));
    }
}
