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
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.remapping.ingest.batch.tasklets.RemappingMetadata;
import uk.ac.ebi.eva.remapping.ingest.batch.tasklets.StoreRemappingMetadataTasklet;

import static uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration.BATCH_TRANSACTION_MANAGER;
import static uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames.STORE_REMAPPING_METADATA_STEP;

@Configuration
public class StoreRemappingMetadataStepConfiguration {

    private final MongoTemplate mongoTemplate;

    private final RemappingMetadata remappingMetadata;

    public StoreRemappingMetadataStepConfiguration(MongoTemplate mongoTemplate, RemappingMetadata remappingMetadata) {
        this.mongoTemplate = mongoTemplate;
        this.remappingMetadata = remappingMetadata;
    }

    @Bean(STORE_REMAPPING_METADATA_STEP)
    public Step buildReportStep(JobRepository jobRepository,
                                @Qualifier(BATCH_TRANSACTION_MANAGER) PlatformTransactionManager transactionManager) {
        StoreRemappingMetadataTasklet tasklet = new StoreRemappingMetadataTasklet(mongoTemplate, remappingMetadata);
        TaskletStep step = new StepBuilder(STORE_REMAPPING_METADATA_STEP, jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
        return step;
    }
}
