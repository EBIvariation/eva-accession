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
 */
package uk.ac.ebi.eva.remapping.ingest.batch.tasklets;

import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.remapping.ingest.parameters.InputParameters;

public class StoreRemappingMetadataTasklet implements Tasklet {

    private final MongoTemplate mongoTemplate;

    private final RemappingMetadata remappingMetadata;

    public StoreRemappingMetadataTasklet(MongoTemplate mongoTemplate, RemappingMetadata remappingMetadata) {
        this.mongoTemplate = mongoTemplate;
        this.remappingMetadata = remappingMetadata;
    }

    @Override
    public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
        mongoTemplate.save(remappingMetadata, "remappingMetadata");
        return RepeatStatus.FINISHED;
    }
}
