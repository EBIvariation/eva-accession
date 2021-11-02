package uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners;

import org.springframework.context.annotation.Bean;
import uk.ac.ebi.eva.accession.pipeline.batch.listeners.AccessioningProgressListener;
import uk.ac.ebi.eva.accession.pipeline.metric.AccessioningMetricComputeImpl;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.PROGRESS_LISTENER;

public class ListenersConfiguration {
    @Bean(PROGRESS_LISTENER)
    public AccessioningProgressListener clusteringProgressListener(InputParameters parameters, MetricCompute metricCompute) {
        return new AccessioningProgressListener(parameters, metricCompute);
    }

    @Bean
    public CountServiceParameters countServiceParameters() {
        return new CountServiceParameters();
    }

    @Bean
    public MetricCompute getClusteringMetricCompute(InputParameters inputParameters) {
        return new AccessioningMetricComputeImpl(inputParameters.getAssemblyAccession(), inputParameters.getProjectAccession());
    }
}
