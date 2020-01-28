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
package uk.ac.ebi.eva.accession.release.configuration.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.configuration.ListenersConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.readers.DeprecatedVariantMongoReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.writers.DeprecatedAccessionWriterConfiguration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DEPRECATED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DEPRECATED_RELEASE_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.PROGRESS_LISTENER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_MAPPED_DEPRECATED_VARIANTS_STEP;

@Configuration
@Import({DeprecatedVariantMongoReaderConfiguration.class,
         DeprecatedAccessionWriterConfiguration.class,
         ListenersConfiguration.class})
public class CreateDeprecatedReleaseStepConfiguration {

    @Autowired
    @Qualifier(DEPRECATED_VARIANT_READER)
    private ItemReader<DbsnpClusteredVariantOperationEntity> deprecatedVariantReader;

    @Autowired
    @Qualifier(DEPRECATED_RELEASE_WRITER)
    private ItemStreamWriter<DbsnpClusteredVariantOperationEntity> accessionWriter;

    @Autowired
    @Qualifier(PROGRESS_LISTENER)
    private StepExecutionListener progressListener;

    @Bean(RELEASE_MAPPED_DEPRECATED_VARIANTS_STEP)
    public Step createDeprecatedReleaseStep(StepBuilderFactory stepBuilderFactory,
                                            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(RELEASE_MAPPED_DEPRECATED_VARIANTS_STEP)
                .<DbsnpClusteredVariantOperationEntity, DbsnpClusteredVariantOperationEntity>chunk(chunkSizeCompletionPolicy)
                .reader(deprecatedVariantReader)
                .writer(accessionWriter)
                .listener(progressListener)
                .build();
        return step;
    }
}
