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
package uk.ac.ebi.eva.accession.release.batch.io.active;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.writer.Options;
import htsjdk.variant.variantcontext.writer.VariantContextWriterBuilder;
import htsjdk.variant.vcf.VCFHeader;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineCount;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamException;
import org.springframework.batch.item.ItemStreamWriter;

import uk.ac.ebi.eva.accession.release.assembly.AssemblyNameRetriever;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.ListContigsStepConfiguration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.ALLELES_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.ASSEMBLY_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.CLUSTERED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.STUDY_ID_KEY;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.SUBMITTED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.SUPPORTED_BY_EVIDENCE_KEY;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.VARIANT_CLASS_KEY;

/**
 * Writes a VCF file for the release of RS IDs mapped against a reference assembly
 *
 * To include the contigs in the meta section it reads the file generated in the previous step
 * {@link ListContigsStepConfiguration}
 */
public class VariantContextWriter implements ItemStreamWriter<VariantContext> {

    private static final Logger logger = LoggerFactory.getLogger(VariantContextWriter.class);

    private File output;

    private String referenceAssembly;

    private htsjdk.variant.variantcontext.writer.VariantContextWriter writer;

    private String contigsFilePath;

    public VariantContextWriter(Path outputPath, String referenceAssembly, String contigsFilePath) {
        this.output = outputPath.toFile();
        this.referenceAssembly = referenceAssembly;
        this.contigsFilePath = contigsFilePath;
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
        addContigs(metaData);
        metaData.add(new VCFHeaderLine("reference", getReferenceAssemblyLine()));
        metaData.add(new VCFInfoHeaderLine(VARIANT_CLASS_KEY, 1, VCFHeaderLineType.String,
                                           "Variant class according to the Sequence Ontology"));
        metaData.add(new VCFInfoHeaderLine(STUDY_ID_KEY, VCFHeaderLineCount.UNBOUNDED, VCFHeaderLineType.String,
                                           "Identifiers of studies that report a variant"));

        metaData.add(new VCFInfoHeaderLine(CLUSTERED_VARIANT_VALIDATED_KEY, 0, VCFHeaderLineType.Flag,
                                           "RS validated flag, present when the RS was validated by any method "
                                           + "as indicated by the dbSNP validation status"));
        metaData.add(new VCFInfoHeaderLine(SUBMITTED_VARIANT_VALIDATED_KEY, 1, VCFHeaderLineType.Integer,
                                           "Number of submitted variants clustered in an RS that were validated by any"
                                           + " method as indicated by the dbSNP validation status"));

        metaData.add(new VCFInfoHeaderLine(ALLELES_MATCH_KEY, 0, VCFHeaderLineType.Flag,
                                           "Alleles mismatch flag, present when some of the submitted variants have "
                                           + "inconsistent allele information. See https://github"
                                           + ".com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP"
                                           + "#alleles-match"));
        metaData.add(new VCFInfoHeaderLine(ASSEMBLY_MATCH_KEY, 0, VCFHeaderLineType.Flag,
                                           "Assembly mismatch flag, present when the reference allele doesn't match "
                                           + "the reference sequence"));
        metaData.add(new VCFInfoHeaderLine(SUPPORTED_BY_EVIDENCE_KEY, 0, VCFHeaderLineType.Flag,
                                           "Lack of evidence flag, present if no submitted variant includes genotype "
                                           + "or frequency information"));
        return metaData;
    }

    private String getReferenceAssemblyLine() {
        AssemblyNameRetriever assemblyNameRetriever = new AssemblyNameRetriever(referenceAssembly);
        Optional<String> assemblyName = assemblyNameRetriever.getAssemblyName();
        String assemblyUrl = assemblyNameRetriever.buildAssemblyHumanReadableUrl();

        if (assemblyName.isPresent()) {
            return "<ID=" + assemblyName.get() + ",accession=" + referenceAssembly + ",URL=" + assemblyUrl + ">";
        } else {
            return "<ID=" + referenceAssembly + ",URL=" + assemblyUrl + ">";
        }
    }

    private void addContigs(Set<VCFHeaderLine> metaData) {
        try {
            BufferedReader bufferedReader = new BufferedReader(new FileReader(contigsFilePath));
            String contigLine;
            while ((contigLine = bufferedReader.readLine()) != null) {
                String[] contigAndName = contigLine.split(",");
                String contig = contigAndName[0];
                String name = contigAndName[1];
                metaData.add(new VCFHeaderLine("contig", "<ID=" + name + ",accession=\"" + contig + "\">"));
            }
        } catch (IOException e) {
            logger.warn("Contigs file not found, VCF header will not have any contigs in the metadata section");
        }
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
