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

import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetric;
import uk.ac.ebi.eva.accession.core.batch.io.SubmittedVariantDeprecationWriter;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

public class DeprecationStepProgressListener
        extends StepListenerSupport<SubmittedVariantEntity, SubmittedVariantEntity> {

    private static final Logger logger = LoggerFactory.getLogger(DeprecationStepProgressListener.class);

    private final SubmittedVariantDeprecationWriter submittedVariantDeprecationWriter;
    private final MetricCompute metricCompute;

    public DeprecationStepProgressListener(SubmittedVariantDeprecationWriter submittedVariantDeprecationWriter,
                                           MetricCompute metricCompute) {
        this.submittedVariantDeprecationWriter = submittedVariantDeprecationWriter;
        this.metricCompute = metricCompute;

    }

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
        int numTotalDeprecatedSS = this.submittedVariantDeprecationWriter.getNumDeprecatedSubmittedEntities();
        int numTotalDeprecatedRS = this.submittedVariantDeprecationWriter.getNumDeprecatedClusteredEntities();
        this.metricCompute.clearCount(ClusteringMetric.SUBMITTED_VARIANTS_DEPRECATED);
        this.metricCompute.addCount(ClusteringMetric.SUBMITTED_VARIANTS_DEPRECATED, numTotalDeprecatedSS);
        this.metricCompute.clearCount(ClusteringMetric.CLUSTERED_VARIANTS_DEPRECATED);
        this.metricCompute.addCount(ClusteringMetric.CLUSTERED_VARIANTS_DEPRECATED, numTotalDeprecatedRS);

        logger.info("{}: SS IDs read = {}, SS IDs deprecated = {}, RS IDs deprecated = {}", stepName, numTotalItemsRead,
                    numTotalDeprecatedSS, numTotalDeprecatedRS);
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        long numTotalDeprecatedSS = this.metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_DEPRECATED);
        long numTotalDeprecatedRS = this.metricCompute.getCount(ClusteringMetric.CLUSTERED_VARIANTS_DEPRECATED);
        this.metricCompute.saveMetricsCountsInDB();

        logger.info("{}: SS IDs read = {}, SS IDs deprecated = {}, RS IDs deprecated = {}", stepExecution.getStepName(),
                    stepExecution.getReadCount(), numTotalDeprecatedSS, numTotalDeprecatedRS);
        return stepExecution.getExitStatus();
    }
}
