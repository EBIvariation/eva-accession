/*
 *
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.accession.core.contigalias;

/**
 * This enum can be used to specify one of the synonyms supported by the contig alias service
 */
public enum ContigAliasNaming {
    GENBANK_SEQUENCE_NAME("genbank.sequence.name"),
    ENA_SEQUENCE_NAME("ena.sequence.name"),
    INSDC("insdc"),  // this is the same as GenBank,
    REFSEQ("refseq"),
    UCSC("ucsc"),
    MD5_CHECKSUM("md5.checksum"),
    TRUNC512_CHECKSUM("trunc512.checksum"),
    NO_REPLACEMENT("no.replacement");  // do not use any particular naming, just keep whatever contig is provided

    private final String text;

    ContigAliasNaming(final String text) {
        this.text = text;
    }

    public String getText() {
        return text;
    }
}
