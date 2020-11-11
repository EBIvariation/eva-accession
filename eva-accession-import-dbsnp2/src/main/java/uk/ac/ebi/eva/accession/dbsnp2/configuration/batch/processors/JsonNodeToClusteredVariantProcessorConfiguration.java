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
package uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.processors;

import com.fasterxml.jackson.databind.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp2.batch.processors.ContigToGenbankReplacerProcessor;
import uk.ac.ebi.eva.accession.dbsnp2.batch.processors.JsonNodeToClusteredVariantProcessor;
import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;

import java.util.Arrays;

import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_PROCESSOR;

/**
 * Configuration to convert a dbSNP JSON line to a clustered variant object.
 */
@Configuration
public class JsonNodeToClusteredVariantProcessorConfiguration {

    private static final Logger logger = LoggerFactory
        .getLogger(JsonNodeToClusteredVariantProcessorConfiguration.class);

    @Bean(name = DBSNP_JSON_VARIANT_PROCESSOR)
    @StepScope
    public ItemProcessor<JsonNode, DbsnpClusteredVariantEntity> dbsnpJsonVariantProcessor(
            InputParameters parameters,
            ContigToGenbankReplacerProcessor contigToGenbankReplacerProcessor,
            JsonNodeToClusteredVariantProcessor jsonNodeToClusteredVariantProcessor) {
        logger.info("Injecting dbsnpVariantProcessor with parameters: {}", parameters);
        CompositeItemProcessor<JsonNode, DbsnpClusteredVariantEntity> compositeProcessor =
            new CompositeItemProcessor<>();
        compositeProcessor.setDelegates(Arrays.asList(jsonNodeToClusteredVariantProcessor,
                                                      contigToGenbankReplacerProcessor));
        return compositeProcessor;
    }

    @Bean
    JsonNodeToClusteredVariantProcessor jsonNodeToClusteredVariantProcessor(InputParameters parameters) {
        return new JsonNodeToClusteredVariantProcessor(parameters.getRefseqAssembly(), parameters.getGenbankAssembly(),
                parameters.getPreviousImportedBuild(), parameters.isIncrementalImport());
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
