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
package uk.ac.ebi.eva.accession.clustering.batch.processors;

import org.springframework.batch.item.ItemProcessor;
import org.springframework.util.Assert;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter;
import uk.ac.ebi.eva.remapping.source.batch.processors.SubmittedVariantToVariantContextProcessor;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.function.Function;
import java.util.stream.Collectors;

public class VariantToSubmittedVariantEntityProcessor implements ItemProcessor<Variant, SubmittedVariantEntity> {

    private String assemblyAccession;

    private Function<ISubmittedVariant, String> hashingFunction;

    public VariantToSubmittedVariantEntityProcessor(String assemblyAccession) {
        if (assemblyAccession == null) {
            throw new IllegalArgumentException("assembly accession must be provided when reading from a VCF");
        }
        this.assemblyAccession = assemblyAccession;
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Override
    public SubmittedVariantEntity process(Variant variant) {
        Assert.isTrue(variant.getMainId() != null && variant.getMainId().matches("ss[0-9]+"),
                      "All variants should have an SS ID as main ID: variant without ID: " + variant.toString());
        Assert.isTrue(variant.getSourceEntries().size() == 1,
                      "All variants should have exactly 1 source entry (each variant should come from a single VCF). " +
                              "SourceEntries found: [" + variant.getSourceEntries().stream()
                                                                .map(e -> e.getAttributes().toString())
                                                                .collect(Collectors.joining(", ")) + "]"
                              + " in variant " + variant.toString());
        long accession = Long.parseLong(variant.getMainId().substring(2));
        VariantSourceEntry sourceEntry = variant.getSourceEntries().iterator().next();

        Long rs = parseRs(sourceEntry.getAttribute(VariantContextWriter.RS_KEY));

        String projectAccession = sourceEntry.getAttribute(VariantContextWriter.PROJECT_KEY);
        Assert.hasText(projectAccession, "project accession should be provided for all variants. Missing in variant "
                + variant.toString());

        SubmittedVariant submittedVariant = new SubmittedVariant(assemblyAccession, 0, projectAccession,
                                                                 variant.getChromosome(), variant.getStart(),
                                                                 variant.getReference(), variant.getAlternate(), rs);

        String hash = hashingFunction.apply(submittedVariant);

        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(accession, hash,
                                                                                   submittedVariant, 1);
        return submittedVariantEntity;
    }

    private Long parseRs(String rsString) {
        Long rs = null;
        if (rsString != null) {
            if (!rsString.startsWith(SubmittedVariantToVariantContextProcessor.RS_PREFIX)) {
                throw new IllegalArgumentException(
                        "if the rs attribute is present (INFO column in VCF), it should be in the format 'rs[0-9]+'. " +
                                "Found: '" + rsString + "'");
            }
            rs = Long.valueOf(rsString.substring(2));
        }
        return rs;
    }
}
