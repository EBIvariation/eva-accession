/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core.batch.io;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ParseException;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.PassThroughLineMapper;
import org.springframework.core.io.UrlResource;

import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;

import java.net.MalformedURLException;

/**
 * Reads a RefSeq Assembly Report to find the contig Synonyms in the supported conventions (Sequence Name, GenBank,
 * RefSeq, Ucsc).
 *
 * @see <a href="ftp://ftp.ncbi.nih.gov/genomes/refseq/vertebrate_mammalian/Canis_lupus/all_assembly_versions/GCF_000002285.3_CanFam3.1/GCF_000002285.3_CanFam3.1_assembly_report.txt">RefSeq Assembly Report</a>
 */
public class AssemblyReportReader implements ItemReader<ContigSynonyms> {

    private static final int SEQNAME_COLUMN = 0;

    private static final int SEQUENCE_ROLE_COLUMN = 1;

    private static final int ASSIGNED_MOLECULE_COLUMN = 2;

    private static final int GENBANK_COLUMN = 4;

    private static final int RELATIONSHIP_COLUMN = 5;

    private static final int REFSEQ_COLUMN = 6;

    private static final int UCSC_COLUMN = 9;

    private static final String IDENTICAL_SEQUENCE = "=";

    private FlatFileItemReader<String> reader;

    public AssemblyReportReader(String url) {
        initializeReader(url);
    }

    private void initializeReader(String url) {
        reader = new FlatFileItemReader<>();
        try {
            reader.setResource(new UrlResource(url));
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("Assembly report file location is invalid: " + url, e);
        }
        reader.setLineMapper(new PassThroughLineMapper());
        reader.open(new ExecutionContext());
    }

    @Override
    public ContigSynonyms read() throws Exception {
        String line = reader.read();
        if (line == null) {
            return null;
        }
        try {
            return getContigSynonyms(line);
        } catch (ArrayIndexOutOfBoundsException exception) {
            throw new ParseException("Error parsing line in Assembly report: '" + line + "'", exception);
        }
    }

    private ContigSynonyms getContigSynonyms(String line) {
        String[] columns = line.split("\t", -1);
        return new ContigSynonyms(columns[SEQNAME_COLUMN],
                                  columns[SEQUENCE_ROLE_COLUMN],
                                  columns[ASSIGNED_MOLECULE_COLUMN],
                                  columns[GENBANK_COLUMN],
                                  columns[REFSEQ_COLUMN],
                                  columns[UCSC_COLUMN],
                                  columns[RELATIONSHIP_COLUMN].equals(IDENTICAL_SEQUENCE));
    }
}
