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

package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter;
import uk.ac.ebi.eva.accession.release.batch.io.merged.MergedVariantContextWriter;
import uk.ac.ebi.eva.accession.release.batch.io.multimap.MultimapVariantContextWriter;
import uk.ac.ebi.eva.accession.release.batch.io.active.VariantContextWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;

import java.nio.file.Path;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MULTIMAP_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_WRITER;

@Configuration
public class VariantContextWriterConfiguration {

    @Bean(RELEASE_WRITER)
    public VariantContextWriter variantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getCurrentIdsReportPath(parameters.getOutputFolder(),
                                                                     parameters.getAssemblyAccession());
        String activeContigsFilePath = ContigWriter.getActiveContigsFilePath(reportPath.toFile().getParent(),
                                                                             parameters.getAssemblyAccession());
        return new VariantContextWriter(reportPath, parameters.getAssemblyAccession(), activeContigsFilePath);
    }

    @Bean(MERGED_RELEASE_WRITER)
    public MergedVariantContextWriter mergedVariantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getMergedIdsReportPath(parameters.getOutputFolder(),
                                                                    parameters.getAssemblyAccession());
        String mergedContigsFilePath = ContigWriter.getMergedContigsFilePath(reportPath.toFile().getParent(),
                                                                             parameters.getAssemblyAccession());
        return new MergedVariantContextWriter(reportPath, parameters.getAssemblyAccession(), mergedContigsFilePath);
    }

    @Bean(MULTIMAP_RELEASE_WRITER)
    public MultimapVariantContextWriter multimapVariantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getMultimapIdsReportPath(parameters.getOutputFolder(),
                                                                      parameters.getAssemblyAccession());
        String activeContigsFilePath = ContigWriter.getMultimapContigsFilePath(reportPath.toFile().getParent(),
                                                                             parameters.getAssemblyAccession());
        return new MultimapVariantContextWriter(reportPath, parameters.getAssemblyAccession(), activeContigsFilePath);
    }

}
