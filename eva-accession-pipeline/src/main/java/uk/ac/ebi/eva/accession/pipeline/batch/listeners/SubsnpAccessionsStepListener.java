package uk.ac.ebi.eva.accession.pipeline.batch.listeners;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

public class SubsnpAccessionsStepListener extends GenericProgressListener<Variant, SubmittedVariantEntity> {
    private final MetricCompute metricCompute;
    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;

    public SubsnpAccessionsStepListener(InputParameters inputParameters, MetricCompute metricCompute,
                                        SubmittedVariantAccessioningService submittedVariantAccessioningService) {
        super(inputParameters.getChunkSize());
        this.metricCompute = metricCompute;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
    }

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        ExitStatus status = super.afterStep(stepExecution);
        submittedVariantAccessioningService.shutDownAccessionGenerator();
        metricCompute.saveMetricsCountsInDB();
        return status;
    }
}
