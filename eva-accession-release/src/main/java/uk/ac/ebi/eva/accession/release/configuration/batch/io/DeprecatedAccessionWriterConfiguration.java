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
package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.batch.io.deprecated.DeprecatedVariantAccessionWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;

import java.nio.file.Path;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_DEPRECATED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_DEPRECATED_RELEASE_WRITER;

@Configuration
public class DeprecatedAccessionWriterConfiguration {

    @Bean(DBSNP_DEPRECATED_RELEASE_WRITER)
    public DeprecatedVariantAccessionWriter deprecatedVariantItemStreamWriterDbsnp(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getDbsnpDeprecatedIdsReportPath(parameters.getOutputFolder(),
                                                                             parameters.getAssemblyAccession());
        return new DeprecatedVariantAccessionWriter(reportPath);
    }

    @Bean(EVA_DEPRECATED_RELEASE_WRITER)
    public DeprecatedVariantAccessionWriter deprecatedVariantItemStreamWriterEva(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getEvaDeprecatedIdsReportPath(parameters.getOutputFolder(),
                                                                           parameters.getAssemblyAccession());
        return new DeprecatedVariantAccessionWriter(reportPath);
    }

}
