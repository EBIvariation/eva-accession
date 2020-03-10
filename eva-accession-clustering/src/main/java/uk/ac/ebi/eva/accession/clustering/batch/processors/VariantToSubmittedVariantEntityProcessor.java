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
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.function.Function;

public class VariantToSubmittedVariantEntityProcessor implements ItemProcessor<Variant, SubmittedVariantEntity> {

    private String assemblyAccession;

    private long submittedVariantAccession;

    private Function<ISubmittedVariant, String> hashingFunction;

    public VariantToSubmittedVariantEntityProcessor(String assemblyAccession) {
        this.assemblyAccession = assemblyAccession;
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        //To simulate the VcfReader is reading the ID column
        submittedVariantAccession = 1000;
    }

    @Override
    public SubmittedVariantEntity process(Variant variant) {
        SubmittedVariant submittedVariant = new SubmittedVariant(assemblyAccession, 0, "",
                variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate(), null);

        String hash = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(submittedVariantAccession, hash,
                                                                                   submittedVariant, 1);
        submittedVariantAccession++;
        return submittedVariantEntity;
    }
}
