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
package uk.ac.ebi.eva.accession.core.fasta;

import htsjdk.samtools.SAMSequenceDictionary;
import htsjdk.samtools.SAMSequenceRecord;
import htsjdk.samtools.reference.FastaSequenceIndexCreator;
import htsjdk.samtools.reference.ReferenceSequence;
import htsjdk.samtools.reference.ReferenceSequenceFile;
import htsjdk.samtools.reference.ReferenceSequenceFileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Reads regions from a given FASTA file, and also creates the associated index and dictionary files if they do not
 * exist.
 */
public class FastaSequenceReader {

    private static final Logger logger = LoggerFactory.getLogger(FastaSequenceReader.class);

    private ReferenceSequenceFile fastaSequenceFile;

    private SAMSequenceDictionary sequenceDictionary;

    public FastaSequenceReader(Path fastaPath) throws IOException {
        checkFastaIsUncompressed(fastaPath);
        fastaSequenceFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(fastaPath, true);
        sequenceDictionary = fastaSequenceFile.getSequenceDictionary();

        if (sequenceDictionary == null) {
            logger.info("Sequence dictionary file not found - creating one...");
            sequenceDictionary = createSequenceDictionary(fastaSequenceFile);
        }
        if (!fastaSequenceFile.isIndexed()) {
            logger.info("Sequence index file not found - creating one...");
            FastaSequenceIndexCreator.create(fastaPath, true);
            fastaSequenceFile = ReferenceSequenceFileFactory.getReferenceSequenceFile(fastaPath, true);
        }
    }

    /**
     * Require the fasta to be uncompressed. Htsjdk seems to support reading and indexing compressed fastas, but for
     * some reason, when asked for a reference, it returns \0 at every position. this test shows the error:
     * uk.ac.ebi.eva.accession.pipeline.io.FastaSequenceReaderTest#htsDoesNotSupportCompressedFastas()
     */
    private void checkFastaIsUncompressed(Path fastaPath) throws IOException {
        if (FileUtils.isGzip(fastaPath.toFile())) {
            throw new IllegalArgumentException("Fasta file should not be compressed: " + fastaPath);
        }
    }

    /**
     * Creates a sequence dictionary based on a given FASTA file. Inspired on the Picard tool with the same name.
     *
     * @param referenceFile File whose dictionary needs to be created
     * @return Dictionary associated with the given reference sequence file
     * @see "https://github.com/broadinstitute/picard/blob/master/src/main/java/picard/sam/CreateSequenceDictionary.java"
     */
    private SAMSequenceDictionary createSequenceDictionary(ReferenceSequenceFile referenceFile) {
        ReferenceSequence referenceSequence;
        final List<SAMSequenceRecord> records = new ArrayList<>();
        final Set<String> sequenceNames = new HashSet<>();

        while ((referenceSequence = referenceFile.nextSequence()) != null) {
            if (sequenceNames.contains(referenceSequence.getName())) {
                throw new IllegalArgumentException(
                        "Sequence name appears more than once in reference: " + referenceSequence.getName());
            }
            sequenceNames.add(referenceSequence.getName());
            records.add(new SAMSequenceRecord(referenceSequence.getName(), referenceSequence.length()));
        }
        return new SAMSequenceDictionary(records);
    }

    /**
     * Get the sequence delimited by the given coordinates from a FASTA file
     *
     * @param contig Sequence contig or chromosome
     * @param start  Sequence start coordinate in the contig
     * @param end    Sequence end coordinate in the contig
     * @return Sequence read from the FASTA file
     * @throws IllegalArgumentException If the coordinates are not correct
     */
    public String getSequence(String contig, long start, long end) throws IllegalArgumentException {
        checkArguments(contig, start, end);

        return fastaSequenceFile.getSubsequenceAt(contig, start, end).getBaseString();
    }

    private void checkArguments(String contig, long start, long end) throws IllegalArgumentException {
        if (end < start) {
            throw new IllegalArgumentException("'end' must be greater or equal than 'start'");
        } else if (start < 1) {
            throw new IllegalArgumentException("'start' and 'end' must be positive numbers");
        } else if (!doesContigExist(contig)) {
            throw new IllegalArgumentException("Sequence " + contig + " not found in reference FASTA file");
        } else {
            int sequenceLengthInFastaFile = sequenceDictionary.getSequence(contig).getSequenceLength();
            if (end > sequenceLengthInFastaFile) {
                throw new IllegalArgumentException(
                        "Variant coordinate " + end + " greater than end of chromosome " + contig + ": " +
                                sequenceLengthInFastaFile);
            }
        }
    }

    public boolean doesContigExist(String contig) {
        return sequenceDictionary.getSequence(contig) != null;
    }

    /**
     * Close the underlying FASTA file
     * @throws Exception If the file cannot be closed
     */
    public void close() throws Exception {
        fastaSequenceFile.close();
    }
}
