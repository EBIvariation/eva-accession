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
package uk.ac.ebi.eva.accession.release.configuration;

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

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EXCLUDE_VARIANTS_LISTENER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.MERGED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.PROGRESS_LISTENER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_MAPPED_MERGED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_PROCESSOR;

@Configuration
public class CreateMergedReleaseStepConfiguration {

    @Autowired
    @Qualifier(MERGED_VARIANT_READER)
    private ItemReader<Variant> variantReader;

    @Autowired
    @Qualifier(RELEASE_PROCESSOR)
    private ItemProcessor<Variant, VariantContext> variantProcessor;

    @Autowired
    @Qualifier(MERGED_RELEASE_WRITER)
    private ItemStreamWriter<VariantContext> accessionWriter;

    @Autowired
    @Qualifier(PROGRESS_LISTENER)
    private StepExecutionListener progressListener;

    @Autowired
    @Qualifier(EXCLUDE_VARIANTS_LISTENER)
    private StepExecutionListener excludeVariantsListener;

    @Bean(RELEASE_MAPPED_MERGED_VARIANTS_STEP)
    public Step createMergedReleaseStep(StepBuilderFactory stepBuilderFactory,
                                        SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(RELEASE_MAPPED_MERGED_VARIANTS_STEP)
                .<Variant, VariantContext>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(accessionWriter)
                .listener(excludeVariantsListener)
                .listener(progressListener)
                .build();
        return step;
    }
}
