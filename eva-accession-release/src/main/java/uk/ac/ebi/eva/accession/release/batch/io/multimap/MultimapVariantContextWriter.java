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
package uk.ac.ebi.eva.accession.release.batch.io.multimap;

import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;

import uk.ac.ebi.eva.accession.release.batch.io.active.VariantContextWriter;

import java.nio.file.Path;
import java.util.Set;

import static uk.ac.ebi.eva.accession.release.batch.io.VariantMongoAggregationReader.MAPPING_WEIGHT_KEY;

public class MultimapVariantContextWriter extends VariantContextWriter {

    public static final String DBSNP_MAP_WEIGHT_DEFINITION_URL =
            "https://www.ncbi.nlm.nih.gov/books/NBK44455/#Build.your_descriptions_of_mapweight_in";

    public MultimapVariantContextWriter(Path outputPath, String referenceAssembly, String multimapContigsFilePath) {
        super(outputPath, referenceAssembly, multimapContigsFilePath);
    }

    @Override
    protected Set<VCFHeaderLine> buildHeaderLines() {
        Set<VCFHeaderLine> vcfHeaderLines = super.buildHeaderLines();
        vcfHeaderLines.add(new VCFInfoHeaderLine(MAPPING_WEIGHT_KEY, 1, VCFHeaderLineType.Integer,
                                                 "mapping weight as defined by dbSNP for database tables at " +
                                                         DBSNP_MAP_WEIGHT_DEFINITION_URL));
        return vcfHeaderLines;
    }

}
