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

package uk.ac.ebi.eva.accession.release.steps.processors;

import htsjdk.variant.variantcontext.VariantContext;
import htsjdk.variant.variantcontext.VariantContextBuilder;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.IVariantSourceEntry;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.ALLELES_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.ASSEMBLY_MATCH_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.STUDY_ID_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.SUBMITTED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.SUPPORTED_BY_EVIDENCE_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.CLUSTERED_VARIANT_VALIDATED_KEY;
import static uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader.VARIANT_CLASS_KEY;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.MERGED_INTO_KEY;

/**
 * Converts an IVariant to a VariantContext.
 *
 * The latter can be serialized using HTSJDK. This processor requires any to-be-serialized property to be set in the
 * IVariant already, such as the alleles.
 */
public class VariantToVariantContextProcessor implements ItemProcessor<IVariant, VariantContext> {

    private final VariantContextBuilder variantContextBuilder;

    public VariantToVariantContextProcessor() {
        this.variantContextBuilder = new VariantContextBuilder();
    }

    @Override
    public VariantContext process(IVariant variant) {
        if (variant.getReference().isEmpty() || variant.getAlternate().isEmpty()) {
            throw new IllegalArgumentException(
                    "VCF specification and HTSJDK forbid empty alleles. Illegal variant: " + variant);
        }
        String[] allelesArray = getAllelesArray(variant);
        convertNamedToSymbolicAlleles(variant, allelesArray);

        VariantContext variantContext = variantContextBuilder
                .chr(variant.getChromosome())
                .start(variant.getStart())
                .stop(getVariantContextStop(variant))
                .id(variant.getMainId())
                .source(variant.getMainId())
                .alleles(allelesArray)
                .attributes(getAttributes(variant))
                .unfiltered()
                .make();

        return variantContext;
    }

    /**
     * Named variants have alleles surrounded by parentheses. Those parentheses will be changed for angular brackets
     * and white spaces will be replaced by underscore so they can be represented in VCF format as symbolic alleles.
     *
     * @param variant
     * @param allelesArray Array containing all alleles
     * @return Array containing all alleles, where parentheses have been replaced by square brackets and white spaces
     * with underscore
     */
    private void convertNamedToSymbolicAlleles(IVariant variant, String[] allelesArray) {
        if (variant.getType() == VariantType.SEQUENCE_ALTERATION && variant.getAlternate().contains("(")) {
            for (int i=0; i < allelesArray.length; i++) {
                allelesArray[i] = allelesArray[i].replace("(", "<").replace(")", ">").replace(" ", "_");
            }
        }
        // TODO change ref too
        variant.
    }

    private Map<String, String> getAttributes(IVariant variant) {
        Map<String, List<String>> attributesList = getSourceEntriesAttributes(variant);

        Map<String, String> attributes = new HashMap<>();
        for (Map.Entry<String, List<String>> attribute : attributesList.entrySet()) {
            switch (attribute.getKey()) {
                case VARIANT_CLASS_KEY:
                case STUDY_ID_KEY:
                case MERGED_INTO_KEY:
                    attributes.put(attribute.getKey(), toUniqueConcatenation(attribute.getValue()));
                    break;
                case ALLELES_MATCH_KEY:
                case ASSEMBLY_MATCH_KEY:
                    if (attribute.getValue().stream().anyMatch(Boolean.toString(false)::equals)) {
                        attributes.put(attribute.getKey(), "");
                    }
                    break;
                case SUPPORTED_BY_EVIDENCE_KEY:
                    if (attribute.getValue().stream().noneMatch(Boolean.toString(true)::equals)) {
                        attributes.put(SUPPORTED_BY_EVIDENCE_KEY, "");
                    }
                    break;
                case CLUSTERED_VARIANT_VALIDATED_KEY:
                    if (attribute.getValue().stream().anyMatch(Boolean.toString(true)::equals)) {
                        attributes.put(CLUSTERED_VARIANT_VALIDATED_KEY, "");
                    }
                    break;
                case SUBMITTED_VARIANT_VALIDATED_KEY:
                    long count = attribute.getValue().stream().filter(Boolean.toString(true)::equals).count();
                    attributes.put(SUBMITTED_VARIANT_VALIDATED_KEY, Long.toString(count));
                    break;
            }
        }
        return attributes;
    }

    private String toUniqueConcatenation(List<String> value) {
        return String.join(",", new HashSet<>(value));
    }

    private Map<String, List<String>> getSourceEntriesAttributes(IVariant variant) {
        Map<String, List<String>> attributes = new HashMap<>();
        for (IVariantSourceEntry sourceEntry : variant.getSourceEntries()) {
            for (Map.Entry<String, String> infoEntry : sourceEntry.getAttributes().entrySet()) {
                if (!attributes.containsKey(infoEntry.getKey())) {
                    attributes.put(infoEntry.getKey(), new ArrayList<>());
                }
                attributes.get(infoEntry.getKey()).add(infoEntry.getValue());
            }
        }
        return attributes;
    }

    private String[] getAllelesArray(IVariant variant) {
        if (variant.getAlternate().contains(",")) {
            throw new IllegalArgumentException("This converter does not allow multiallelic variants");
        }
        return new String[]{variant.getReference(), variant.getAlternate()};
    }

    private long getVariantContextStop(IVariant variant) {
        return variant.getStart() + variant.getReference().length() - 1;
    }
}
