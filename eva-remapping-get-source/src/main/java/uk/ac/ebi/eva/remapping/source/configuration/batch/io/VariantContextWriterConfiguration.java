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

package uk.ac.ebi.eva.remapping.source.configuration.batch.io;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.remapping.source.batch.io.VariantContextWriter;
import uk.ac.ebi.eva.remapping.source.parameters.InputParameters;
import uk.ac.ebi.eva.remapping.source.parameters.ReportPathResolver;
import uk.ac.ebi.eva.remapping.source.configuration.BeanNames;

import java.nio.file.Path;

@Configuration
public class VariantContextWriterConfiguration {

    @Bean(BeanNames.EVA_SUBMITTED_VARIANT_WRITER)
    public VariantContextWriter evaVariantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getEvaReportPath(parameters.getOutputFolder(),
                                                              parameters.getAssemblyAccession());
        return new VariantContextWriter(reportPath, parameters.getAssemblyAccession());
    }

    @Bean(BeanNames.DBSNP_SUBMITTED_VARIANT_WRITER)
    public VariantContextWriter dbsnpVariantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getDbsnpReportPath(parameters.getOutputFolder(),
                                                                parameters.getAssemblyAccession());
        return new VariantContextWriter(reportPath, parameters.getAssemblyAccession());
    }
}
