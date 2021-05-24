/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.configuration.batch.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.remapping.ingest.batch.processors.ContigToGenbankReplacerProcessor;
import uk.ac.ebi.eva.remapping.ingest.batch.processors.VariantToSubmittedVariantEntityRemappedProcessor;
import uk.ac.ebi.eva.remapping.ingest.parameters.InputParameters;

import java.util.Arrays;

import static uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames.COMPOSITE_VARIANT_PROCESSOR;

@Configuration
public class VariantProcessorConfiguration {

    @Bean(COMPOSITE_VARIANT_PROCESSOR)
    @StepScope
    public ItemProcessor<IVariant, SubmittedVariantEntity> compositeVariantProcessor(
            ContigToGenbankReplacerProcessor contigToGenbankReplacerProcessor,
            VariantToSubmittedVariantEntityRemappedProcessor variantToSubmittedVariantEntityRemappedProcessor) {
        CompositeItemProcessor<IVariant, SubmittedVariantEntity> compositeProcessor = new CompositeItemProcessor<>();
        compositeProcessor.setDelegates(Arrays.asList(contigToGenbankReplacerProcessor,
                                                      variantToSubmittedVariantEntityRemappedProcessor));
        return compositeProcessor;
    }

    @Bean
    public ContigToGenbankReplacerProcessor contigReplacerProcessor(ContigMapping contigMapping) {
        return new ContigToGenbankReplacerProcessor(contigMapping);
    }

    @Bean
    public ContigMapping contigMapping(InputParameters parameters) throws Exception {
        return new ContigMapping(parameters.getAssemblyReportUrl());
    }

    @Bean
    public VariantToSubmittedVariantEntityRemappedProcessor variantToSubmittedVariantEntityRemappedProcessor(
            InputParameters inputParameters) {
        return new VariantToSubmittedVariantEntityRemappedProcessor(inputParameters.getAssemblyAccession(),
                                                                    inputParameters.getRemappedFrom());
    }

}
