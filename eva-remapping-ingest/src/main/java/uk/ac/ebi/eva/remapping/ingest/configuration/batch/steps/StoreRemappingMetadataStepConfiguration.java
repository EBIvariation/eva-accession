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
package uk.ac.ebi.eva.remapping.ingest.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.remapping.ingest.batch.listeners.RemappingIngestCounts;
import uk.ac.ebi.eva.remapping.ingest.batch.tasklets.RemappingMetadata;
import uk.ac.ebi.eva.remapping.ingest.batch.tasklets.StoreRemappingMetadataTasklet;
import uk.ac.ebi.eva.remapping.ingest.parameters.InputParameters;

import java.io.IOException;

import static uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames.STORE_REMAPPING_METADATA_STEP;

@Configuration
@EnableBatchProcessing
public class StoreRemappingMetadataStepConfiguration {

    private final MongoTemplate mongoTemplate;

    private final InputParameters inputParameters;

    public StoreRemappingMetadataStepConfiguration(MongoTemplate mongoTemplate, InputParameters inputParameters) {
        this.mongoTemplate = mongoTemplate;
        this.inputParameters = inputParameters;
    }

    @Bean(STORE_REMAPPING_METADATA_STEP)
    public Step buildReportStep(StepBuilderFactory stepBuilderFactory) throws IOException {
        StoreRemappingMetadataTasklet tasklet = new StoreRemappingMetadataTasklet(mongoTemplate, inputParameters);
        TaskletStep step = stepBuilderFactory.get(STORE_REMAPPING_METADATA_STEP)
                                             .tasklet(tasklet)
                                             .build();
        return step;
    }
}
