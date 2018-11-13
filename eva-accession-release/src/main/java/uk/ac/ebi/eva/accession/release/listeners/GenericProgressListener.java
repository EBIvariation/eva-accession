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
 */

package uk.ac.ebi.eva.accession.release.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.batch.core.scope.context.ChunkContext;

import java.util.List;

public class GenericProgressListener<I, O> extends StepListenerSupport<I, O> {

    private static final Logger logger = LoggerFactory.getLogger(GenericProgressListener.class);

    private long chunkSize;

    private long numItemsRead;

    public GenericProgressListener(long chunkSize) {
        this.chunkSize = chunkSize;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        logger.debug("Starting a step");
        numItemsRead = stepExecution.getReadCount();
    }

    @Override
    public void beforeChunk(ChunkContext context) {
        logger.debug("Starting a chunk");
    }

    @Override
    public void beforeRead() {
        if (numItemsRead % chunkSize == 0) {
            logger.debug("About to read item {}", numItemsRead);
        }
        numItemsRead++;
    }

    @Override
    public void afterRead(I itemRead) {
        if (numItemsRead % chunkSize == 0) {
            logger.debug("Read {} items", numItemsRead);
        }
    }

    @Override
    public void beforeWrite(List<? extends O> items) {
        logger.debug("About to write chunk");
    }

    @Override
    public void afterWrite(List<? extends O> items) {
        if (items.size() > 0) {
            O lastItem = items.get(items.size() - 1);
            logger.debug("Written chunk of {} items. Last item was {}: {}", items.size(), lastItem.toString());
        } else {
            logger.debug("Written chunk of 0 items.");
        }
    }

    @Override
    public void afterChunk(ChunkContext context) {
        String stepName = context.getStepContext().getStepName();
        long numTotalItemsRead = context.getStepContext().getStepExecution().getReadCount();
        long numTotalItemsWritten = context.getStepContext().getStepExecution().getWriteCount();

        logger.info("{}: Items read = {}, items written = {}", stepName, numTotalItemsRead, numTotalItemsWritten);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.debug("Finished a step");
        return stepExecution.getExitStatus();
    }
}