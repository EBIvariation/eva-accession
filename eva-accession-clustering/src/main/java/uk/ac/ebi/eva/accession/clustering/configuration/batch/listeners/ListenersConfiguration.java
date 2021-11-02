package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringProgressListener;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetricComputeImpl;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {

    @Bean(PROGRESS_LISTENER)
    public ClusteringProgressListener clusteringProgressListener(InputParameters parameters, MetricCompute metricCompute) {
        return new ClusteringProgressListener(parameters, metricCompute);
    }

    @Bean
    public CountServiceParameters countServiceParameters() {
        return new CountServiceParameters();
    }

    @Bean
    public MetricCompute getClusteringMetricCompute(InputParameters inputParameters) {
        return new ClusteringMetricComputeImpl(inputParameters.getAssemblyAccession());
    }
}
