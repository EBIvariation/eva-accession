package uk.ac.ebi.eva.accession.pipeline.batch.listeners;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

public class AccessioningProgressListener extends GenericProgressListener<Variant, SubmittedVariantEntity> {
    private final MetricCompute metricCompute;

    public AccessioningProgressListener(InputParameters inputParameters, MetricCompute metricCompute) {
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
