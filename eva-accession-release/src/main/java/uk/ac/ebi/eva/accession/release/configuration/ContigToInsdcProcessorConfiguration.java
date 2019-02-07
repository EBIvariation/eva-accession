/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.steps.processors.ContigToInsdcProcessor;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_TO_INSDC_PROCESSOR;

@Configuration
public class ContigToInsdcProcessorConfiguration {

    @Bean(name = CONTIG_TO_INSDC_PROCESSOR)
    public ContigToInsdcProcessor contigProcessor(ContigMapping contigMapping) {
        return new ContigToInsdcProcessor(contigMapping);
    }

    @Bean
    ContigMapping contigMapping(InputParameters parameters) throws Exception {
        return new ContigMapping(parameters.getAssemblyReportUrl());
    }
}
