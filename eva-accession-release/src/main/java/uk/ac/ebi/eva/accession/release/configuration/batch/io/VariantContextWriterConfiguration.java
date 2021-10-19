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

import org.springframework.data.mongodb.core.MongoOperations;

import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.release.batch.io.ReleaseRecordWriter;
import uk.ac.ebi.eva.accession.release.batch.io.contig.ContigWriter;
import uk.ac.ebi.eva.accession.release.batch.io.merged.MergedVariantContextWriter;
import uk.ac.ebi.eva.accession.release.batch.io.multimap.MultimapVariantContextWriter;
import uk.ac.ebi.eva.accession.release.batch.io.active.VariantContextWriter;
import uk.ac.ebi.eva.accession.release.batch.processors.ContextNucleotideAdditionProcessor;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;

import java.nio.file.Path;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MULTIMAP_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MULTIMAP_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.INCREMENTAL_RELEASE_WRITER;

@Configuration
public class VariantContextWriterConfiguration {

    @Bean(DBSNP_RELEASE_WRITER)
    public VariantContextWriter variantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getDbsnpCurrentIdsReportPath(parameters.getOutputFolder(),
                                                                          parameters.getAssemblyAccession());
        String activeContigsFilePath = ContigWriter.getDbsnpActiveContigsFilePath(reportPath.toFile().getParent(),
                                                                                  parameters.getAssemblyAccession());
        return new VariantContextWriter(reportPath, parameters.getAssemblyAccession(), activeContigsFilePath);
    }

    @Bean(DBSNP_MERGED_RELEASE_WRITER)
    public MergedVariantContextWriter mergedVariantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getDbsnpMergedIdsReportPath(parameters.getOutputFolder(),
                                                                         parameters.getAssemblyAccession());
        String mergedContigsFilePath = ContigWriter.getDbsnpMergedContigsFilePath(reportPath.toFile().getParent(),
                                                                                  parameters.getAssemblyAccession());
        return new MergedVariantContextWriter(reportPath, parameters.getAssemblyAccession(), mergedContigsFilePath);
    }

    @Bean(DBSNP_MULTIMAP_RELEASE_WRITER)
    public MultimapVariantContextWriter multimapVariantContextWriter(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getDbsnpMultimapIdsReportPath(parameters.getOutputFolder(),
                                                                           parameters.getAssemblyAccession());
        String activeContigsFilePath = ContigWriter.getDbsnpMultimapContigsFilePath(reportPath.toFile().getParent(),
                                                                                    parameters.getAssemblyAccession());
        return new MultimapVariantContextWriter(reportPath, parameters.getAssemblyAccession(), activeContigsFilePath);
    }

    @Bean(EVA_RELEASE_WRITER)
    public VariantContextWriter variantContextWriterEva(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getEvaCurrentIdsReportPath(parameters.getOutputFolder(),
                                                                        parameters.getAssemblyAccession());
        String activeContigsFilePath = ContigWriter.getEvaActiveContigsFilePath(reportPath.toFile().getParent(),
                                                                                parameters.getAssemblyAccession());
        return new VariantContextWriter(reportPath, parameters.getAssemblyAccession(), activeContigsFilePath);
    }

    @Bean(EVA_MERGED_RELEASE_WRITER)
    public MergedVariantContextWriter mergedVariantContextWriterEva(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getEvaMergedIdsReportPath(parameters.getOutputFolder(),
                                                                       parameters.getAssemblyAccession());
        String mergedContigsFilePath = ContigWriter.getEvaMergedContigsFilePath(reportPath.toFile().getParent(),
                                                                                parameters.getAssemblyAccession());
        return new MergedVariantContextWriter(reportPath, parameters.getAssemblyAccession(), mergedContigsFilePath);
    }

    @Bean(EVA_MULTIMAP_RELEASE_WRITER)
    public MultimapVariantContextWriter multimapVariantContextWriterEva(InputParameters parameters) {
        Path reportPath = ReportPathResolver.getEvaMultimapIdsReportPath(parameters.getOutputFolder(),
                                                                         parameters.getAssemblyAccession());
        String activeContigsFilePath = ContigWriter.getEvaMultimapContigsFilePath(reportPath.toFile().getParent(),
                                                                                  parameters.getAssemblyAccession());
        return new MultimapVariantContextWriter(reportPath, parameters.getAssemblyAccession(), activeContigsFilePath);
    }

    @Bean(INCREMENTAL_RELEASE_WRITER)
    public ReleaseRecordWriter incrementalReleaseWriter(MongoOperations mongoOperations,
                                                        SubmittedVariantAccessioningRepository submittedVariantAccessioningRepository,
                                                        ClusteredVariantAccessioningRepository clusteredVariantAccessioningRepository,
                                                        FastaSynonymSequenceReader fastaSynonymSequenceReader,
                                                        InputParameters parameters) {
        ContextNucleotideAdditionProcessor contextNucleotideAdditionProcessor =
                new ContextNucleotideAdditionProcessor(fastaSynonymSequenceReader);
        return new ReleaseRecordWriter(mongoOperations, submittedVariantAccessioningRepository,
                clusteredVariantAccessioningRepository, contextNucleotideAdditionProcessor, parameters.getAssemblyAccession());
    }

}
