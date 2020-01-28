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

package uk.ac.ebi.eva.accession.pipeline.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.steps.processors.ContigToGenbankReplacerProcessor;
import uk.ac.ebi.eva.accession.pipeline.steps.processors.ExcludeStructuralVariantsProcessor;
import uk.ac.ebi.eva.commons.core.models.IVariant;

import java.util.Arrays;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.COMPOSITE_VARIANT_PROCESSOR;

/**
 * Configuration to inject a VariantProcessor as a bean.
 */
@Configuration
public class VariantProcessorConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(VariantProcessorConfiguration.class);

    @Bean(COMPOSITE_VARIANT_PROCESSOR)
    @StepScope
    public ItemProcessor<IVariant, IVariant> compositeVariantProcessor(
            InputParameters inputParameters,
            ContigToGenbankReplacerProcessor contigToGenbankReplacerProcessor,
            ExcludeStructuralVariantsProcessor excludeStructuralVariantsProcessor) {
        logger.info("Injecting dbsnpVariantProcessor with parameters: {}", inputParameters);
        CompositeItemProcessor<IVariant, IVariant> compositeProcessor = new CompositeItemProcessor<>();
        compositeProcessor.setDelegates(Arrays.asList(excludeStructuralVariantsProcessor,
                                                      contigToGenbankReplacerProcessor));
        return compositeProcessor;
    }

    @Bean
    ExcludeStructuralVariantsProcessor excludeStructuralVariantsProcessor() {
        return new ExcludeStructuralVariantsProcessor();
    }

    @Bean
    ContigToGenbankReplacerProcessor contigReplacerProcessor(ContigMapping contigMapping) {
        return new ContigToGenbankReplacerProcessor(contigMapping);
    }

    @Bean
    ContigMapping contigMapping(InputParameters parameters) throws Exception {
        return new ContigMapping(parameters.getAssemblyReportUrl());
    }
}
