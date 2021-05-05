/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.ingest.remapped.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantRemappedEntity;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.ingest.remapped.configuration.BeanNames.INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP;
import static uk.ac.ebi.eva.ingest.remapped.configuration.BeanNames.REMAPPED_SUBMITTED_VARIANTS_WRITER;
import static uk.ac.ebi.eva.ingest.remapped.configuration.BeanNames.VARIANT_TO_SUBMITTED_VARIANT_ENTITY_REMAPPED_PROCESSOR;
import static uk.ac.ebi.eva.ingest.remapped.configuration.BeanNames.VCF_READER;

@Configuration
@EnableBatchProcessing
public class IngestRemappedFromVcfStepConfiguration {

    @Bean(INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP)
    public Step ingestRemappedFromVcf(
            @Qualifier(VCF_READER) ItemReader<Variant> vcfReader,
            @Qualifier(VARIANT_TO_SUBMITTED_VARIANT_ENTITY_REMAPPED_PROCESSOR) ItemProcessor<Variant, SubmittedVariantRemappedEntity> processor,
            @Qualifier(REMAPPED_SUBMITTED_VARIANTS_WRITER) ItemWriter<SubmittedVariantRemappedEntity> submittedVariantWriter,
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP)
                                             .<Variant, SubmittedVariantRemappedEntity>chunk(chunkSizeCompletionPolicy)
                                             .reader(vcfReader)
                                             .processor(processor)
                                             .writer(submittedVariantWriter)
//                                             .listener(progressListener)
                                             .build();
        return step;
    }

}
