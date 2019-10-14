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
package uk.ac.ebi.eva.accession.dbsnp2.listeners;

import org.codehaus.jackson.JsonNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.item.ExecutionContext;
import uk.ac.ebi.eva.accession.core.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;

public class ImportDbsnpJsonVariantsStepProgressListener extends GenericProgressListener<JsonNode,
        DbsnpClusteredVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ImportDbsnpJsonVariantsStepProgressListener.class);

    private ImportCounts importCounts;

    public ImportDbsnpJsonVariantsStepProgressListener(long chunkSize, ImportCounts importCounts) {
        super(chunkSize);
        this.importCounts = importCounts;
    }

    @Override
    public void beforeStep(StepExecution stepExecution) {
        super.beforeStep(stepExecution);
        // if the number of variants written is stored in the execution context, it means this job is restarting and we
        // should initialize the import counts with the values stored in the execution context
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        if (executionContext.containsKey(ImportCounts.CLUSTERED_VARIANTS_WRITTEN)) {
            importCounts.setClusteredVariantsWritten(executionContext.getLong(ImportCounts.CLUSTERED_VARIANTS_WRITTEN));
            importCounts.setOperationsWritten(executionContext.getLong(ImportCounts.OPERATIONS_WRITTEN));
        }
    }

    @Override
    public void afterChunk(ChunkContext context) {
        super.afterChunk(context);
        ExecutionContext executionContext = context.getStepContext().getStepExecution().getExecutionContext();
        addImportCountsToExecutionContext(executionContext);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExitStatus status = super.afterStep(stepExecution);
        String stepName = stepExecution.getStepName();
        long numTotalItemsRead = stepExecution.getReadCount();
        logger.info("Step {} finished: Items read = {}, rs written = {}, operations written = {}",
            stepName, numTotalItemsRead,
            importCounts.getClusteredVariantsWritten(), importCounts.getOperationsWritten());
        // add import counts to execution context, so they can be retrieved later if the job is restarted
        ExecutionContext executionContext = stepExecution.getExecutionContext();
        addImportCountsToExecutionContext(executionContext);
        return status;
    }

    private void addImportCountsToExecutionContext(ExecutionContext executionContext) {
        executionContext.putLong(ImportCounts.CLUSTERED_VARIANTS_WRITTEN, importCounts.getClusteredVariantsWritten());
        executionContext.putLong(ImportCounts.OPERATIONS_WRITTEN, importCounts.getOperationsWritten());
    }
}
