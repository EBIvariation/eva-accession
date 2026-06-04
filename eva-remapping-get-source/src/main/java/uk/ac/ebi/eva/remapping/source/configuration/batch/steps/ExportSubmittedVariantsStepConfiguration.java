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
package uk.ac.ebi.eva.remapping.source.configuration.batch.steps;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
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
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.core.batch.policies.IllegalStartSkipPolicy;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.remapping.source.configuration.BeanNames;
import uk.ac.ebi.eva.remapping.source.configuration.batch.io.SubmittedVariantMongoReaderConfiguration;
import uk.ac.ebi.eva.remapping.source.configuration.batch.io.VariantContextWriterConfiguration;
import uk.ac.ebi.eva.remapping.source.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.remapping.source.configuration.batch.policies.PoliciesConfiguration;
import uk.ac.ebi.eva.remapping.source.configuration.batch.processors.SubmittedVariantsProcessorConfiguration;

import static uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration.BATCH_TRANSACTION_MANAGER;

@Configuration
@Import({SubmittedVariantMongoReaderConfiguration.class,
        SubmittedVariantsProcessorConfiguration.class,
        VariantContextWriterConfiguration.class,
        ListenersConfiguration.class,
        PoliciesConfiguration.class})
public class ExportSubmittedVariantsStepConfiguration {

    @Bean(BeanNames.EXPORT_EVA_SUBMITTED_VARIANTS_STEP)
    public Step exportEvaSubmittedVariantsStep(
            JobRepository jobRepository,
            @Qualifier(BATCH_TRANSACTION_MANAGER) PlatformTransactionManager transactionManager,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Autowired @Qualifier(BeanNames.EVA_SUBMITTED_VARIANT_READER) ItemReader<SubmittedVariantEntity> variantReader,
            @Autowired @Qualifier(BeanNames.SUBMITTED_VARIANT_PROCESSOR) ItemProcessor<SubmittedVariantEntity, VariantContext> variantProcessor,
            @Autowired @Qualifier(BeanNames.EVA_SUBMITTED_VARIANT_WRITER) ItemStreamWriter<VariantContext> accessionWriter,
            @Autowired @Qualifier(BeanNames.PROGRESS_LISTENER) StepExecutionListener progressListener,
            @Autowired @Qualifier(BeanNames.EXCLUDE_VARIANTS_LISTENER) StepExecutionListener excludeVariantsListener,
            @Autowired IllegalStartSkipPolicy illegalStartSkipPolicy) {
        TaskletStep step = new StepBuilder(BeanNames.EXPORT_EVA_SUBMITTED_VARIANTS_STEP, jobRepository)
                .<SubmittedVariantEntity, VariantContext>chunk(chunkSizeCompletionPolicy, transactionManager)
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

    @Bean(BeanNames.EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP)
    public Step exportDbsnpSubmittedVariantsStep(
            JobRepository jobRepository,
            @Qualifier(BATCH_TRANSACTION_MANAGER) PlatformTransactionManager transactionManager,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Autowired @Qualifier(BeanNames.DBSNP_SUBMITTED_VARIANT_READER) ItemReader<DbsnpSubmittedVariantEntity> variantReader,
            @Autowired @Qualifier(BeanNames.SUBMITTED_VARIANT_PROCESSOR) ItemProcessor<SubmittedVariantEntity, VariantContext> variantProcessor,
            @Autowired @Qualifier(BeanNames.DBSNP_SUBMITTED_VARIANT_WRITER) ItemStreamWriter<VariantContext> accessionWriter,
            @Autowired @Qualifier(BeanNames.PROGRESS_LISTENER) StepExecutionListener progressListener,
            @Autowired @Qualifier(BeanNames.EXCLUDE_VARIANTS_LISTENER) StepExecutionListener excludeVariantsListener,
            @Autowired IllegalStartSkipPolicy illegalStartSkipPolicy) {
        TaskletStep step = new StepBuilder(BeanNames.EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP, jobRepository)
                .<SubmittedVariantEntity, VariantContext>chunk(chunkSizeCompletionPolicy, transactionManager)
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
