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

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;

public class AccessionSummaryWriter {

    private File output;

    private FastaSequenceReader fastaSequenceReader;

    private boolean headerWritten;

    private final FileWriter fileWriter;

    public AccessionSummaryWriter(File output, FastaSequenceReader fastaSequenceReader) throws IOException {
        this.output = output;
        this.fastaSequenceReader = fastaSequenceReader;
        this.headerWritten = false;
        this.fileWriter = new FileWriter(output);
    }

    public void write(Map<Long, ISubmittedVariant> accessions) throws IOException {
        if (!headerWritten) {
            writeHeader();
            headerWritten = true;
        }
        for (Map.Entry<Long, ISubmittedVariant> variant : accessions.entrySet()) {
            writeVariant(variant.getKey(), variant.getValue());
        }
        fileWriter.flush();
    }

    private void writeHeader() throws IOException {
        fileWriter.write("##fileformat=VCFv4.1\n");
        fileWriter.write("#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO\n");
    }

    private void writeVariant(Long id, ISubmittedVariant variant) throws IOException {
        String variantLine = String.join("\t",
                                         variant.getContig(),
                                         Long.toString(variant.getStart()),
                                         "rs" + Long.toString(id),
                                         variant.getReferenceAllele(),
                                         variant.getAlternateAllele(),
                                         ".", ".", ".");
        fileWriter.write(variantLine + "\n");
    }
}
