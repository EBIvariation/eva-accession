/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.remapping.source.batch.processors;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.HashMap;
import java.util.Map;

import static uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter.CREATED_DATE;
import static uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter.RS_KEY;
import static uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter.SS_HASH;
import static uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter.PROJECT_KEY;
import static uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter.TAXONOMY;

/**
 * Converts an SubmittedVariantEntity to a VariantContext.
 *
 * The latter can be serialized using HTSJDK. This processor requires any to-be-serialized property to be set in the
 * SubmittedVariantEntity already, such as the alleles.
 */
public class SubmittedVariantToVariantContextProcessor implements ItemProcessor<SubmittedVariantEntity, VariantContext> {

    public static final String RS_PREFIX = "rs";

    public static final String SS_PREFIX = "ss";

    private final VariantContextBuilder variantContextBuilder;

    public SubmittedVariantToVariantContextProcessor() {
        this.variantContextBuilder = new VariantContextBuilder();
    }

    @Override
    public VariantContext process(SubmittedVariantEntity variant) {
        if (variant.getReferenceAllele().isEmpty() || variant.getAlternateAllele().isEmpty()) {
            throw new IllegalArgumentException(
                    "VCF specification and HTSJDK forbid empty alleles. Illegal variant: " + variant);
        }
        String[] allelesArray = getAllelesArray(variant);
        String sequenceName = variant.getContig();

        VariantContext variantContext = variantContextBuilder
                .chr(sequenceName)
                .start(variant.getStart())
                .stop(getVariantContextStop(variant))
                .id(SS_PREFIX + variant.getAccession())
                .alleles(allelesArray)
                .attributes(getAttributes(variant))
                .unfiltered()
                .make();

        return variantContext;
    }

    private Map<String, String> getAttributes(SubmittedVariantEntity variant) {
        Map<String, String> attributes = new HashMap<>();
        attributes.put(SS_HASH, variant.getHashedMessage());
        if (variant.getClusteredVariantAccession() != null) {
            attributes.put(RS_KEY, RS_PREFIX + variant.getClusteredVariantAccession());
        }
        attributes.put(PROJECT_KEY, replaceInvalidCharacters(variant.getProjectAccession()));
        String taxonomyAccession = String.valueOf(variant.getTaxonomyAccession());
        attributes.put(TAXONOMY, replaceInvalidCharacters(taxonomyAccession));
        String createdDate = variant.getCreatedDate().toString();
        attributes.put(CREATED_DATE, replaceInvalidCharacters(createdDate));
        return attributes;
    }

    /**
     * In VCF, in the INFO column, keys and values can not have spaces, commas, semicolons or equal signs. Specially
     * the study IDs from dbSNP are likely to contain some of those letters.
     *
     * See section 1.2 of VCFv4.3 (https://samtools.github.io/hts-specs/VCFv4.3.pdf)
     */
    private String replaceInvalidCharacters(String infoValues) {
        return infoValues.replaceAll("[%]", "%25")
                         .replaceAll("[,]", "%2C")
                         .replaceAll("[;]", "%3B")
                         .replaceAll("[=]", "%3D");
    }

    private String[] getAllelesArray(SubmittedVariantEntity variant) {
        if (variant.getAlternateAllele().contains(",")) {
            throw new IllegalArgumentException("This converter does not allow multiallelic variants");
        }
        return new String[]{variant.getReferenceAllele(), variant.getAlternateAllele()};
    }

    private long getVariantContextStop(SubmittedVariantEntity variant) {
        return variant.getStart() + variant.getReferenceAllele().length() - 1;
    }
}
