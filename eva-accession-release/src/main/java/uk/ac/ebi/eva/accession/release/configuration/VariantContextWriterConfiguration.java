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

package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.io.VariantContextWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import java.io.File;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_WRITER;

@Configuration
public class VariantContextWriterConfiguration {

    @Bean(RELEASE_WRITER)
    public VariantContextWriter variantContextWriter(InputParameters parameters) {
        return new VariantContextWriter(new File(parameters.getOutputVcf()), parameters.getAssemblyAccession());
    }

    @Bean(MERGED_RELEASE_WRITER)
    public VariantContextWriter mergedVariantContextWriter(InputParameters parameters) {
        return new VariantContextWriter(new File(parameters.getOutputVcfMerged()), parameters.getAssemblyAccession());
    }

}
