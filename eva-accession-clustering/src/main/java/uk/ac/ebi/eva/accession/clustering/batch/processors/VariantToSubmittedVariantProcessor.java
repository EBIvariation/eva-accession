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
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.ArrayList;
import java.util.List;

public class VariantToSubmittedVariantProcessor implements ItemProcessor<List<Variant>, List<SubmittedVariant>> {

    @Override
    public List<SubmittedVariant> process(List<Variant> variants) {
        List<SubmittedVariant> submittedVariants = new ArrayList<>();
        for (Variant variant : variants) {
            SubmittedVariant submittedVariant = new SubmittedVariant(variant.getReference(), 0, "",
                    variant.getChromosome(), variant.getStart(), variant.getReference(), variant.getAlternate(), null);
            submittedVariants.add(submittedVariant);
        }
        return submittedVariants;
    }
}
