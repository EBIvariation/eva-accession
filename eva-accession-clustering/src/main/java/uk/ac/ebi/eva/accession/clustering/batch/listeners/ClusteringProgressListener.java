/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.batch.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

public class ClusteringProgressListener extends GenericProgressListener<Variant, SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(ClusteringProgressListener.class);

    private final ClusteringCounts clusteringCounts;

    public ClusteringProgressListener(long chunkSize, ClusteringCounts clusteringCounts) {
        super(chunkSize);
        this.clusteringCounts = clusteringCounts;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExitStatus status = super.afterStep(stepExecution);

        String stepName = stepExecution.getStepName();
        long numTotalItemsRead = stepExecution.getReadCount();
        logger.info("Step {} finished: Items (ss) read = {}, rs created = {}, rs updated = {}, " +
                            "rs merge operations = {}, ss kept unclustered = {}, " +
                        "ss clustered = {}, ss updated rs merged = {}, ss update operations = {}",
                stepName, numTotalItemsRead,
                clusteringCounts.getClusteredVariantsCreated(),
                clusteringCounts.getClusteredVariantsUpdated(),
                clusteringCounts.getClusteredVariantsMergeOperationsWritten(),
                clusteringCounts.getSubmittedVariantsKeptUnclustered(),
                clusteringCounts.getSubmittedVariantsClustered(),
                clusteringCounts.getSubmittedVariantsUpdatedRs(),
                clusteringCounts.getSubmittedVariantsUpdateOperationWritten());
        return status;
    }
}
