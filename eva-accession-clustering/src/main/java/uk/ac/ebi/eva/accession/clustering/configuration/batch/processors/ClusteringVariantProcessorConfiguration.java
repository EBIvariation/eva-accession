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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.processors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.clustering.batch.processors.VariantToSubmittedVariantEntityProcessor;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.VARIANT_TO_SUBMITTED_VARIANT_PROCESSOR;

@Configuration
public class ClusteringVariantProcessorConfiguration {

    @Autowired
    InputParameters inputParameters;

    @Bean(VARIANT_TO_SUBMITTED_VARIANT_PROCESSOR)
    public VariantToSubmittedVariantEntityProcessor variantToSubmittedVariantProcessor() {
        return new VariantToSubmittedVariantEntityProcessor(inputParameters.getAssemblyAccession());
    }
}
