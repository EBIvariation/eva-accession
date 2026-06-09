/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames;

import static uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration.BATCH_TRANSACTION_MANAGER;

@Configuration
public class DeprecateStudySubmittedVariantsStepConfiguration {

    @Autowired
    @Qualifier(BeanNames.STUDY_SUMITTED_VARIANTS_READER)
    private ItemStreamReader<SubmittedVariantEntity> studySubmittedVariantsReader;

    @Autowired
    @Qualifier(BeanNames.STUDY_DEPRECATION_WRITER)
    private ItemWriter<SubmittedVariantEntity> submittedVariantDeprecationWriter;

    @Autowired
    @Qualifier(BeanNames.DEPRECATION_PROGRESS_LISTENER)
    private StepExecutionListener progressListener;

    @Bean(BeanNames.DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP)
    public Step deprecateClusteredVariantsStep(JobRepository jobRepository,
                                               @Qualifier(BATCH_TRANSACTION_MANAGER)
                                               PlatformTransactionManager transactionManager,
                                               SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = new StepBuilder(BeanNames.DEPRECATE_STUDY_SUBMITTED_VARIANTS_STEP, jobRepository)
                .<SubmittedVariantEntity, SubmittedVariantEntity>chunk(chunkSizeCompletionPolicy, transactionManager)
                .reader(studySubmittedVariantsReader)
                .writer(submittedVariantDeprecationWriter)
                .listener(progressListener)
                .build();
        return step;
    }
}
