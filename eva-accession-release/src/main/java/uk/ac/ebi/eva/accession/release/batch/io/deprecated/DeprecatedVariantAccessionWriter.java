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
package uk.ac.ebi.eva.accession.release.batch.io.deprecated;

import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.List;

/**
 * Writes the accessions of historical variants to a flat file.
 */
public class DeprecatedVariantAccessionWriter implements ItemStreamWriter<EventDocument<IClusteredVariant, Long,
        ? extends ClusteredVariantInactiveEntity>> {

    private final File output;

    private PrintWriter printWriter;

    public DeprecatedVariantAccessionWriter(Path outputPath) {
        this.output = outputPath.toFile();
    }

    public File getOutput() {
        return output;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        try {
            printWriter = new PrintWriter(new FileWriter(output));
        } catch (IOException e) {
            throw new ItemStreamException(e);
        }
    }

    @Override
    public void write(
            List<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>> variants) {
        for (EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity> variant : variants) {
            printWriter.println("rs" + variant.getAccession());
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        printWriter.close();
    }

}
