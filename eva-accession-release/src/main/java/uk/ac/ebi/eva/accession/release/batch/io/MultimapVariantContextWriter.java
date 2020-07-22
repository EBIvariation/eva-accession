/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.release.batch.io;

import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

import java.nio.file.Path;
import java.util.Set;

import static uk.ac.ebi.eva.accession.release.batch.io.VariantMongoAggregationReader.WEIGHT_KEY;

public class MultimapVariantContextWriter extends VariantContextWriter {

    public MultimapVariantContextWriter(Path outputPath, String referenceAssembly, String multimapContigsFilePath) {
        super(outputPath, referenceAssembly, multimapContigsFilePath);
    }

    @Override
    protected Set<VCFHeaderLine> buildHeaderLines() {
        Set<VCFHeaderLine> vcfHeaderLines = super.buildHeaderLines();
        vcfHeaderLines.add(new VCFInfoHeaderLine(WEIGHT_KEY, 1, VCFHeaderLineType.Integer,
                                                 "TODO definition of mapping weight (count per chr or per asm?)"));
        return vcfHeaderLines;
    }

}
