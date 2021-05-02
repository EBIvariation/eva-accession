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
package uk.ac.ebi.eva.remapping.source.batch.io;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.File;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Writes a VCF file that can be used as input for our remapping pipeline.
 */
public class VariantContextWriter implements ItemStreamWriter<VariantContext> {

    private static final Logger logger = LoggerFactory.getLogger(VariantContextWriter.class);

    public static final String RS_KEY = "RS";

    public static final String PROJECT_KEY = "PROJECT";

    private final File output;

    private final String referenceAssembly;

    private htsjdk.variant.variantcontext.writer.VariantContextWriter writer;

    public VariantContextWriter(Path outputPath, String referenceAssembly) {
        this.output = outputPath.toFile();
        this.referenceAssembly = referenceAssembly;
    }

    public File getOutput() {
        return output;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        VariantContextWriterBuilder builder = new VariantContextWriterBuilder();
        writer = builder
                .setOutputFile(output)
                .setOutputFileType(VariantContextWriterBuilder.OutputType.VCF)
                .unsetOption(Options.INDEX_ON_THE_FLY)
                .build();

        Set<VCFHeaderLine> metaData = buildHeaderLines();
        writer.writeHeader(new VCFHeader(metaData));
    }

    protected Set<VCFHeaderLine> buildHeaderLines() {
        Set<VCFHeaderLine> metaData = new HashSet<>();
        metaData.add(new VCFHeaderLine("reference", getReferenceAssemblyLine()));
        metaData.add(new VCFInfoHeaderLine(RS_KEY, 1, VCFHeaderLineType.String,
                                           "RS ID where this SS ID is clustered"));
        metaData.add(new VCFInfoHeaderLine(PROJECT_KEY, 1, VCFHeaderLineType.String,
                                           "PROJECT ID associated with this SS ID"));
        return metaData;
    }

    private String getReferenceAssemblyLine() {
        return referenceAssembly;
    }

    @Override
    public void write(List<? extends VariantContext> variantContexts) throws Exception {
        for (VariantContext variantContext : variantContexts) {
            writer.add(variantContext);
        }
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {
    }

    @Override
    public void close() throws ItemStreamException {
        writer.close();
    }

}
