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

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.steps.processors.VariantProcessor;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.VARIANT_PROCESSOR;

/**
 * Configuration to inject a VariantProcessor as a bean.
 */
@Configuration
public class VariantProcessorConfiguration {

    @Bean(VARIANT_PROCESSOR)
    @StepScope
    public VariantProcessor variantProcessor(InputParameters inputParameters) {
        String assemblyAccession = inputParameters.getAssemblyAccession();
        int taxonomyAccession = inputParameters.getTaxonomyAccession();
        String projectAccession = inputParameters.getProjectAccession();

        return new VariantProcessor(assemblyAccession, taxonomyAccession, projectAccession);
    }
}
