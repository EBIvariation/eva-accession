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
package uk.ac.ebi.eva.accession.release.io;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import java.io.File;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class VariantContextWriter implements ItemStreamWriter<VariantContext> {

    private File output;

    private String referenceAssembly;

    private htsjdk.variant.variantcontext.writer.VariantContextWriter writer;

    public VariantContextWriter(File output, String referenceAssembly) {
        this.output = output;
        this.referenceAssembly = referenceAssembly;
    }

    @Override
    public void open(ExecutionContext executionContext) throws ItemStreamException {
        VariantContextWriterBuilder builder = new VariantContextWriterBuilder();
        writer = builder.setOutputFile(output).unsetOption(Options.INDEX_ON_THE_FLY).build();

        Set<VCFHeaderLine> metaData = new HashSet<>();
        metaData.add(new VCFHeaderLine("reference", referenceAssembly));
        metaData.add(new VCFInfoHeaderLine("VC", 1, VCFHeaderLineType.String, "Variant class"));
        metaData.add(new VCFInfoHeaderLine("SID", VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.String,
                                           "Identifiers of studies that report a variant"));
        writer.writeHeader(new VCFHeader(metaData));
    }

    @Override
    public void update(ExecutionContext executionContext) throws ItemStreamException {

    }

    @Override
    public void close() throws ItemStreamException {
        writer.close();
    }

    @Override
    public void write(List<? extends VariantContext> variantContexts) throws Exception {
        for (VariantContext variantContext : variantContexts) {
            writer.add(variantContext);
        }
    }
}
