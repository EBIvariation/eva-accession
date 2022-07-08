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
package uk.ac.ebi.eva.accession.deprecate.batch.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

public class DeprecationStepProgressListener
        extends StepListenerSupport<SubmittedVariantEntity, SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DeprecationStepProgressListener.class);

    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.debug("Starting a step");
    }

    @Override
    public void beforeChunk(ChunkContext context) {
        logger.debug("Starting a chunk");
    }

    @Override
    public void afterChunk(ChunkContext context) {
        String stepName = context.getStepContext().getStepName();
        long numTotalItemsRead = context.getStepContext().getStepExecution().getReadCount();
        long numTotalItemsWritten = context.getStepContext().getStepExecution().getWriteCount();

        logger.info("{}: SS IDs read = {}, SS IDs deprecated = {}", stepName, numTotalItemsRead, numTotalItemsWritten);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Step {} finished: SS IDs read = {}, SS IDs deprecated = {}",
                    stepExecution.getStepName(), stepExecution.getReadCount(), stepExecution.getWriteCount());
        return stepExecution.getExitStatus();
    }
}
