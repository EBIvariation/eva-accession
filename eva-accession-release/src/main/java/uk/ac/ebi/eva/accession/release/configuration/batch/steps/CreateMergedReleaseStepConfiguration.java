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
package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

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

import uk.ac.ebi.eva.accession.release.configuration.batch.io.MergedVariantMongoReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.VariantContextWriterConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.processors.ReleaseProcessorConfiguration;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EXCLUDE_VARIANTS_LISTENER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.PROGRESS_LISTENER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_EVA_MAPPED_MERGED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_PROCESSOR;

@Configuration
@Import({MergedVariantMongoReaderConfiguration.class,
         ReleaseProcessorConfiguration.class,
         VariantContextWriterConfiguration.class,
         ListenersConfiguration.class})
public class CreateMergedReleaseStepConfiguration {

    @Autowired
    @Qualifier(PROGRESS_LISTENER)
    private StepExecutionListener progressListener;

    @Autowired
    @Qualifier(EXCLUDE_VARIANTS_LISTENER)
    private StepExecutionListener excludeVariantsListener;

    @Bean(RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP)
    public Step createMergedReleaseStepDbsnp(
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(DBSNP_MERGED_VARIANT_READER)
                    ItemReader<Variant> variantReader,
            @Qualifier(RELEASE_PROCESSOR)
                    ItemProcessor<Variant, VariantContext> variantProcessor,
            @Qualifier(DBSNP_MERGED_RELEASE_WRITER)
                    ItemStreamWriter<VariantContext> accessionWriter) {
        TaskletStep step = stepBuilderFactory.get(RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP)
                .<Variant, VariantContext>chunk(chunkSizeCompletionPolicy)
                .reader(variantReader)
                .processor(variantProcessor)
                .writer(accessionWriter)
                .listener(excludeVariantsListener)
                .listener(progressListener)
                .build();
        return step;
    }

    @Bean(RELEASE_EVA_MAPPED_MERGED_VARIANTS_STEP)
    public Step createMergedReleaseStepEva(
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(EVA_MERGED_VARIANT_READER)
                    ItemReader<Variant> variantReader,
            @Qualifier(RELEASE_PROCESSOR)
                    ItemProcessor<Variant, VariantContext> variantProcessor,
            @Qualifier(EVA_MERGED_RELEASE_WRITER)
                    ItemStreamWriter<VariantContext> accessionWriter) {
        TaskletStep step = stepBuilderFactory.get(RELEASE_EVA_MAPPED_MERGED_VARIANTS_STEP)
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
