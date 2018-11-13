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

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

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

        VariantContext variantContext = variantContextBuilder
                .chr(variant.getChromosome())
                .start(variant.getStart())
                .stop(getVariantContextStop(variant))
                .id(variant.getMainId())
                .source(variant.getMainId())
                .alleles(allelesArray)
                .attributes(getUniqueAttributes(variant))
                .unfiltered()
                .make();

        return variantContext;
    }

    private Map<String, String> getUniqueAttributes(IVariant variant) {
        Map<String, Set<String>> attributes = new HashMap<>();
        for (IVariantSourceEntry sourceEntry : variant.getSourceEntries()) {
            for (Map.Entry<String, String> infoEntry : sourceEntry.getAttributes().entrySet()) {
                if (!attributes.containsKey(infoEntry.getKey())) {
                    attributes.put(infoEntry.getKey(), new HashSet<>());
                }
                attributes.get(infoEntry.getKey()).add(infoEntry.getValue());
            }
        }
        return attributes
                .entrySet()
                .stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> String.join(",", e.getValue())));
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
