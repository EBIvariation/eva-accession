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
package uk.ac.ebi.eva.remapping.ingest.batch.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;

import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

public class RemappingIngestionProgressListener extends GenericProgressListener<Variant, SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(RemappingIngestCounts.class);

    private final RemappingIngestCounts remappingIngestCounts;

    public RemappingIngestionProgressListener(long chunkSize, RemappingIngestCounts remappingIngestCounts) {
        super(chunkSize);
        this.remappingIngestCounts = remappingIngestCounts;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExitStatus status = super.afterStep(stepExecution);

        String stepName = stepExecution.getStepName();
        long numTotalItemsRead = stepExecution.getReadCount();
        logger.info("Step {} finished: Items (remapped ss) read = {}, ss ingested = {}, ss skipped (duplicate) = {}",
                    stepName,
                    numTotalItemsRead,
                    remappingIngestCounts.getRemappedVariantsIngested(),
                    remappingIngestCounts.getRemappedVariantsSkipped());
        return status;
    }
}
