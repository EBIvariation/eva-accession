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

import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.ALLELES_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.ASSEMBLY_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.STUDY_ID_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.SUBMITTED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.SUPPORTED_BY_EVIDENCE_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.VARIANT_CLASS_KEY;

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
        writer = builder
                .setOutputFile(output)
                .setOutputFileType(VariantContextWriterBuilder.OutputType.VCF)
                .unsetOption(Options.INDEX_ON_THE_FLY)
                .build();

        Set<VCFHeaderLine> metaData = new HashSet<>();
        metaData.add(new VCFHeaderLine("reference", referenceAssembly));
        metaData.add(new VCFInfoHeaderLine(VARIANT_CLASS_KEY, 1, VCFHeaderLineType.String,
                                           "Variant class according to the Sequence Ontology"));
        metaData.add(new VCFInfoHeaderLine(STUDY_ID_KEY, VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.String,
                                           "Identifiers of studies that report a variant"));

        metaData.add(new VCFInfoHeaderLine(VALIDATED_KEY, 0, VCFHeaderLineType.Flag,
                                           "If an RS was validated by any method according to dbSNP validation "
                                           + "status, this flag will be present. If it was not validated by any "
                                           + "method, this flag won't be present."));
        metaData.add(new VCFInfoHeaderLine(ALLELES_MATCH_KEY, 0, VCFHeaderLineType.Flag,
                                           "Alleles mismatch flag, present when some of the submitted variants has "
                                           + "inconsistent allele information. See https://github"
                                           + ".com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP"
                                           + "#alleles-match"));

        metaData.add(new VCFInfoHeaderLine(SUBMITTED_VARIANT_VALIDATED_KEY, VCFHeaderLineCount.UNBOUNDED,
                                           VCFHeaderLineType.Integer,
                                           "Number of submitted variants clustered in an RS that were validated by any"
                                           + " method according to dbSNP validation status, the value of this field "
                                           + "is 1, and 0 otherwise"));
        metaData.add(new VCFInfoHeaderLine(ASSEMBLY_MATCH_KEY, 0, VCFHeaderLineType.Flag,
                                           "Assembly mismatch flag, present when the reference allele doesn't match "
                                           + "the reference sequence"));
        metaData.add(new VCFInfoHeaderLine(SUPPORTED_BY_EVIDENCE_KEY, 0, VCFHeaderLineType.Flag,
                                           "Lack of evidence. If this flag is present it means that no submitted "
                                           + "variant included genotype or frequency information"));
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
