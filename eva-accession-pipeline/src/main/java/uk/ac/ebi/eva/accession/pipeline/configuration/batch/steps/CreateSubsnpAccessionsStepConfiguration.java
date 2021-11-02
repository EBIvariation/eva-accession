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
package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionWriter;
import uk.ac.ebi.eva.accession.pipeline.batch.policies.InvalidVariantSkipPolicy;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSION_WRITER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.COMPOSITE_VARIANT_PROCESSOR;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.PROGRESS_LISTENER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.VARIANT_READER;

@Configuration
@EnableBatchProcessing
public class CreateSubsnpAccessionsStepConfiguration {

    @Autowired
    @Qualifier(VARIANT_READER)
    private ItemReader<Variant> variantReader;

    @Autowired
    @Qualifier(COMPOSITE_VARIANT_PROCESSOR)
    private ItemProcessor<IVariant, IVariant> variantProcessor;

    @Autowired
    @Qualifier(ACCESSION_WRITER)
    private AccessionWriter accessionWriter;

    @Autowired
    @Qualifier(PROGRESS_LISTENER)
    private StepExecutionListener progressListener;

    @Autowired
    private InvalidVariantSkipPolicy invalidVariantSkipPolicy;

    @Bean(CREATE_SUBSNP_ACCESSION_STEP)
    public Step createSubsnpAccessionStep(StepBuilderFactory stepBuilderFactory,
                                          SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(CREATE_SUBSNP_ACCESSION_STEP)
                .<IVariant, IVariant>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(accessionWriter)
                .faultTolerant()
                .skipPolicy(invalidVariantSkipPolicy)
                .listener(progressListener)
                .build();
        return step;
    }
}