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

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

public class ClusteringProgressListener extends GenericProgressListener<Variant, SubmittedVariantEntity> {
    private final MetricCompute metricCompute;

    public ClusteringProgressListener(InputParameters inputParameters, MetricCompute metricCompute) {
        super(inputParameters.getChunkSize());
        this.metricCompute = metricCompute;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExitStatus status = super.afterStep(stepExecution);
        metricCompute.saveMetricsCountsInDB();
        return status;
    }
}
