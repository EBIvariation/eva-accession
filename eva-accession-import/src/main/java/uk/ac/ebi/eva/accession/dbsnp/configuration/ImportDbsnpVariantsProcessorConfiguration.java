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
package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.persistence.ProjectAccessionMapping;
import uk.ac.ebi.eva.accession.dbsnp.processors.AssemblyCheckerProcessor;
import uk.ac.ebi.eva.accession.dbsnp.processors.ContigReplacerProcessor;
import uk.ac.ebi.eva.accession.dbsnp.processors.SubSnpNoHgvsToDbsnpVariantsWrapperProcessor;
import uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_VARIANT_PROCESSOR;

@Configuration
public class ImportDbsnpVariantsProcessorConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ImportDbsnpVariantsProcessorConfiguration.class);

    @Bean(name = DBSNP_VARIANT_PROCESSOR)
    @StepScope
    ItemProcessor<SubSnpNoHgvs, DbsnpVariantsWrapper> dbsnpVariantProcessor(
            InputParameters parameters,
            ContigReplacerProcessor contigReplacerProcessor,
            AssemblyCheckerProcessor assemblyCheckerProcessor,
            SubSnpNoHgvsToDbsnpVariantsWrapperProcessor subSnpNoHgvsToDbsnpVariantsWrapperProcessor,
            SubmittedVariantDeclusterProcessor submittedVariantDeclusterProcessor)
            throws Exception {
        logger.info("Injecting dbsnpVariantProcessor with parameters: {}", parameters);
        CompositeItemProcessor<SubSnpNoHgvs, DbsnpVariantsWrapper> compositeProcessor = new CompositeItemProcessor<>();
        compositeProcessor.setDelegates(Arrays.asList(contigReplacerProcessor,
                                                      assemblyCheckerProcessor,
                                                      subSnpNoHgvsToDbsnpVariantsWrapperProcessor,
                                                      submittedVariantDeclusterProcessor));
        return compositeProcessor;
    }

    @Bean
    ContigReplacerProcessor contigReplacerProcessor(ContigMapping contigMapping, InputParameters parameters) {
        return new ContigReplacerProcessor(contigMapping, parameters.getAssemblyAccession());
    }

    @Bean
    ContigMapping contigMapping(InputParameters parameters) throws Exception {
        return new ContigMapping(parameters.getAssemblyReportUrl());
    }

    @Bean
    AssemblyCheckerProcessor assemblyCheckerProcessor(FastaSynonymSequenceReader fastaSynonymSequenceReader) {
        return new AssemblyCheckerProcessor(fastaSynonymSequenceReader);
    }

    @Bean
    FastaSynonymSequenceReader fastaSynonymSequenceReader(ContigMapping contigMapping, InputParameters parameters)
            throws IOException {
        Path referenceFastaFile = Paths.get(parameters.getFasta());
        return new FastaSynonymSequenceReader(contigMapping, referenceFastaFile);
    }

    @Bean
    SubSnpNoHgvsToDbsnpVariantsWrapperProcessor subSnpNoHgvsToDbsnpVariantsWrapperProcessor(
            InputParameters parameters, FastaSynonymSequenceReader fastaSynonymSequenceReader,
            List<ProjectAccessionMapping> projectAccessionMappings) {
        return new SubSnpNoHgvsToDbsnpVariantsWrapperProcessor(parameters.getAssemblyAccession(),
                                                               fastaSynonymSequenceReader, projectAccessionMappings);
    }

    @Bean
    List<ProjectAccessionMapping> projectAccessionMappings(MongoTemplate mongoTemplate) {
        return mongoTemplate.findAll(ProjectAccessionMapping.class);
    }

    @Bean
    SubmittedVariantDeclusterProcessor submittedVariantDeclusterProcessor() {
        return new SubmittedVariantDeclusterProcessor();
    }

}
