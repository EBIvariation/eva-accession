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
package uk.ac.ebi.eva.accession.remapping.configuration.batch.steps;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.remapping.batch.policies.IllegalStartSkipPolicy;
import uk.ac.ebi.eva.accession.remapping.configuration.batch.io.SubmittedVariantMongoReaderConfiguration;
import uk.ac.ebi.eva.accession.remapping.configuration.batch.io.VariantContextWriterConfiguration;
import uk.ac.ebi.eva.accession.remapping.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.remapping.configuration.batch.processors.SubmittedVariantsProcessorConfiguration;

import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.DBSNP_SUBMITTED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.DBSNP_SUBMITTED_VARIANT_WRITER;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EVA_SUBMITTED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EVA_SUBMITTED_VARIANT_WRITER;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EXCLUDE_VARIANTS_LISTENER;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EXPORT_EVA_SUBMITTED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.PROGRESS_LISTENER;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.SUBMITTED_VARIANT_PROCESSOR;

@Configuration
@Import({SubmittedVariantMongoReaderConfiguration.class,
        SubmittedVariantsProcessorConfiguration.class,
        VariantContextWriterConfiguration.class,
        ListenersConfiguration.class})
public class ExportSubmittedVariantsStepConfiguration {

    @Bean(EXPORT_EVA_SUBMITTED_VARIANTS_STEP)
    public Step exportEvaSubmittedVariantsStep(
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Autowired @Qualifier(EVA_SUBMITTED_VARIANT_READER) ItemReader<SubmittedVariantEntity> variantReader,
            @Autowired @Qualifier(SUBMITTED_VARIANT_PROCESSOR) ItemProcessor<SubmittedVariantEntity, VariantContext> variantProcessor,
            @Autowired @Qualifier(EVA_SUBMITTED_VARIANT_WRITER) ItemStreamWriter<VariantContext> accessionWriter,
            @Autowired @Qualifier(PROGRESS_LISTENER) StepExecutionListener progressListener,
            @Autowired @Qualifier(EXCLUDE_VARIANTS_LISTENER) StepExecutionListener excludeVariantsListener,
            @Autowired IllegalStartSkipPolicy illegalStartSkipPolicy) {
        TaskletStep step = stepBuilderFactory.get(EXPORT_EVA_SUBMITTED_VARIANTS_STEP)
                .<SubmittedVariantEntity, VariantContext>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(accessionWriter)
                .faultTolerant()
                .skipPolicy(illegalStartSkipPolicy)
                .listener(excludeVariantsListener)
                .listener(progressListener)
                .build();
        return step;
    }

    @Bean(EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP)
    public Step exportDbsnpSubmittedVariantsStep(
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Autowired @Qualifier(DBSNP_SUBMITTED_VARIANT_READER) ItemReader<DbsnpSubmittedVariantEntity> variantReader,
            @Autowired @Qualifier(SUBMITTED_VARIANT_PROCESSOR) ItemProcessor<SubmittedVariantEntity, VariantContext> variantProcessor,
            @Autowired @Qualifier(DBSNP_SUBMITTED_VARIANT_WRITER) ItemStreamWriter<VariantContext> accessionWriter,
            @Autowired @Qualifier(PROGRESS_LISTENER) StepExecutionListener progressListener,
            @Autowired @Qualifier(EXCLUDE_VARIANTS_LISTENER) StepExecutionListener excludeVariantsListener,
            @Autowired IllegalStartSkipPolicy illegalStartSkipPolicy) {
        TaskletStep step = stepBuilderFactory.get(EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP)
                .<SubmittedVariantEntity, VariantContext>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(accessionWriter)
                .faultTolerant()
                .skipPolicy(illegalStartSkipPolicy)
                .listener(excludeVariantsListener)
                .listener(progressListener)
                .build();
        return step;
    }
}
