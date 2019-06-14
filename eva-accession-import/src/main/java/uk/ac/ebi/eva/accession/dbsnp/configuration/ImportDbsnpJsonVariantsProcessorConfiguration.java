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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpJson;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp.processors.DbsnpJsonProcessor;
import uk.ac.ebi.eva.commons.core.models.IVariant;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_JSON_VARIANT_PROCESSOR;

/**
 * Processor configuration provides DbsnpJsonProcessor
 */
@Configuration
public class ImportDbsnpJsonVariantsProcessorConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ImportDbsnpJsonVariantsProcessorConfiguration.class);

    @Bean(name = DBSNP_JSON_VARIANT_PROCESSOR)
    @StepScope
    public ItemProcessor<DbsnpJson, IVariant> dbsnpJsonVariantProcessor(
            InputParameters parameters)
            throws Exception {
        logger.info("Injecting dbsnpJsonVariantProcessor with parameters: {}", parameters);
        return new DbsnpJsonProcessor();
    }
}
