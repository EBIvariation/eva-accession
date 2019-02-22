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
package uk.ac.ebi.eva.accession.deprecate.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;

import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECABLE_CLUSTERED_VARIANTS_READER;
import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATE_CLUSTERED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATION_WRITER;

@Configuration
@EnableBatchProcessing
public class DeprecateClusteredVariantsStepConfiguration {

    @Autowired
    @Qualifier(DEPRECABLE_CLUSTERED_VARIANTS_READER)
    private ItemStreamReader<DbsnpClusteredVariantEntity> reader;

    @Autowired
    @Qualifier(DEPRECATION_WRITER)
    private ItemWriter<DbsnpClusteredVariantEntity> writer;

    @Bean(DEPRECATE_CLUSTERED_VARIANTS_STEP)
    public Step deprecateClusteredVariantsStep(StepBuilderFactory stepBuilderFactory,
                                               SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(DEPRECATE_CLUSTERED_VARIANTS_STEP)
                .<DbsnpClusteredVariantEntity, DbsnpClusteredVariantEntity>chunk(chunkSizeCompletionPolicy)
                .reader(reader)
                .writer(writer)
                .build();
        return step;
    }
}
