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
 *
 */
package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.ASSEMBLY_CHECK_STEP_LISTENER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_VARIANT_PROCESSOR;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_VARIANT_READER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.DBSNP_VARIANT_WRITER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_PROGRESS_LISTENER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_STEP;

@Configuration
@EnableBatchProcessing
public class ImportDbsnpVariantsStepConfiguration {

    @Autowired
    @Qualifier(DBSNP_VARIANT_READER)
    private ItemReader<SubSnpNoHgvs> variantReader;

    @Autowired
    @Qualifier(DBSNP_VARIANT_PROCESSOR)
    private ItemProcessor<SubSnpNoHgvs, DbsnpVariantsWrapper> variantProcessor;

    @Autowired
    @Qualifier(DBSNP_VARIANT_WRITER)
    private ItemWriter<DbsnpVariantsWrapper> accessionWriter;

    @Autowired
    @Qualifier(ASSEMBLY_CHECK_STEP_LISTENER)
    private StepExecutionListener assemblyCheckStepListener;

    @Autowired
    @Qualifier(IMPORT_DBSNP_VARIANTS_PROGRESS_LISTENER)
    private StepExecutionListener importDbsnpVariantsProgressListener;

    @Bean(IMPORT_DBSNP_VARIANTS_STEP)
    public Step createSubsnpAccessionStep(StepBuilderFactory stepBuilderFactory,
                                          SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(IMPORT_DBSNP_VARIANTS_STEP)
                .<SubSnpNoHgvs, DbsnpVariantsWrapper>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(accessionWriter)
                .listener(assemblyCheckStepListener)
                .listener(importDbsnpVariantsProgressListener)
                .build();
        return step;
    }
}
