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
package uk.ac.ebi.eva.accession.dbsnp2.configuration;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;

import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_READER;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_PROCESSOR;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_WRITER;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_PROGRESS_LISTENER;

/**
 * Configuration for dbSNP JSON import flow step
 */
@Configuration
@EnableBatchProcessing
public class ImportDbsnpJsonVariantsStepConfiguration {

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_READER)
    private FlatFileItemReader<JsonNode> variantReader;

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_PROCESSOR)
    private ItemProcessor<JsonNode, DbsnpClusteredVariantEntity> variantProcessor;

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_WRITER)
    private ItemWriter<DbsnpClusteredVariantEntity> variantWriter;

    @Autowired
    @Qualifier(IMPORT_DBSNP_JSON_VARIANTS_PROGRESS_LISTENER)
    private StepExecutionListener importDbsnpJsonVariantsProgressListener;


    @Bean(IMPORT_DBSNP_JSON_VARIANTS_STEP)
    public Step importDbsnpJsonVariantsStep(StepBuilderFactory stepBuilderFactory,
                                            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        return stepBuilderFactory.get(IMPORT_DBSNP_JSON_VARIANTS_STEP)
            .<JsonNode, DbsnpClusteredVariantEntity>chunk(chunkSizeCompletionPolicy)
            .reader(variantReader)
            .processor(variantProcessor)
            .writer(variantWriter)
            .listener(importDbsnpJsonVariantsProgressListener)
            .build();
    }
}
