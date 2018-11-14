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
package uk.ac.ebi.eva.accession.core.io;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;

import java.io.IOException;
import java.nio.file.Path;

public class FastaSynonymSequenceReader extends FastaSequenceReader {

    private ContigMapping contigMapping;

    public FastaSynonymSequenceReader(ContigMapping contigMapping, Path fastaPath) throws IOException {
        super(fastaPath);
        this.contigMapping = contigMapping;
    }

    @Override
    public boolean doesContigExist(String contig) {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contig);

        if (contigSynonyms == null) {
            throw new IllegalArgumentException(
                    "Contig '" + contig + "' not found in the assembly report");
        }

        if (contigSynonyms.isIdenticalGenBankAndRefSeq()){
            return super.doesContigExist(contigSynonyms.getSequenceName())
                    || super.doesContigExist(contigSynonyms.getGenBank())
                    || super.doesContigExist(contigSynonyms.getRefSeq())
                    || super.doesContigExist(contigSynonyms.getUcsc())
                    || super.doesContigExist(contigSynonyms.getAssignedMolecule());
        } else {
            return super.doesContigExist(contig);
        }
    }

    @Override
    public String getSequence(String contig, long start, long end) {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(contig);

        if (contigSynonyms == null) {
            throw new IllegalArgumentException(
                    "Contig '" + contig + "' not found in the assembly report");
        }

        if (contigSynonyms.isIdenticalGenBankAndRefSeq()) {
            return getSequenceUsingSynonyms(contigSynonyms, start, end);
        } else {
            String sequence = getSequenceIgnoringMissingContig(contig, start, end);
            if (sequence == null) {
                throw new IllegalArgumentException("Contig " + contig + " not found in the FASTA file");
            } else {
                return sequence;
            }

        }
    }

    private String getSequenceUsingSynonyms(ContigSynonyms contigSynonyms, long start, long end) {
        String sequence;
        if ((sequence = getSequenceIgnoringMissingContig(contigSynonyms.getSequenceName(), start, end)) != null) {
            return sequence;
        }
        if ((sequence = getSequenceIgnoringMissingContig(contigSynonyms.getGenBank(), start, end)) != null) {
            return sequence;
        }
        if ((sequence = getSequenceIgnoringMissingContig(contigSynonyms.getRefSeq(), start, end)) != null) {
            return sequence;
        }
        if ((sequence = getSequenceIgnoringMissingContig(contigSynonyms.getUcsc(), start, end)) != null) {
            return sequence;
        }
        if ((sequence = getSequenceIgnoringMissingContig(contigSynonyms.getAssignedMolecule(), start, end)) != null) {
            return sequence;
        }
        throw new IllegalArgumentException("Contig " + contigSynonyms.toString() + " not found in the FASTA file");
    }

    /**
     * Tries to retrieve a sequence from a FASTA file. If the sequence can't be found using that name, it could be that
     * the FASTA uses a different nomenclature; in that case a synonym could be used.
     *
     * But if the sequence can be found and the coordinates are not valid, the error is unrecoverable and an exception
     * is thrown.
     *
     * @param contig Name of the sequence to be retrieved
     * @param start Start coordinate of the sequence to be retrieved
     * @param end End coordinate of the sequence to be retrieved
     * @return The sequence or null if not found
     * @throws IllegalArgumentException if the sequence name can be found in the FASTA but the coordinates are too large
     */
    private String getSequenceIgnoringMissingContig(String contig, long start, long end) {
        if (contig != null && sequenceDictionary.getSequence(contig) != null) {
            try {
                return super.getSequence(contig, start, end);
            } catch (IllegalArgumentException sequenceUnavailable) {
                /*
                The same exception type could be caused because the contig was not found, or the requested coordinates
                were greater than the last position in the contig. In order to differentiate between them, we make
                another request for position 1 of the sequence.
                */
                try {
                    super.getSequence(contig, 1, 1);
                } catch (IllegalArgumentException contigMissing) {
                    return null;
                }

                // The exception is only thrown when the sequence name was found but the coordinates are not valid.
                throw sequenceUnavailable;
            }
        } else {
            return null;
        }
    }
}
