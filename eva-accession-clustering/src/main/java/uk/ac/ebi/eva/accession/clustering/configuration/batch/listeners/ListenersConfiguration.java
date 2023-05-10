package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringProgressListener;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetricCompute;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.metrics.configuration.MetricConfiguration;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
@Import({MetricConfiguration.class})
public class ListenersConfiguration {

    @Bean(PROGRESS_LISTENER)
    public ClusteringProgressListener clusteringProgressListener(InputParameters parameters, MetricCompute metricCompute) {
        return new ClusteringProgressListener(parameters, metricCompute);
    }

    @Bean
    public MetricCompute getClusteringMetricCompute(CountServiceParameters countServiceParameters,
                                                    @Qualifier("COUNT_STATS_REST_TEMPLATE") RestTemplate restTemplate,
                                                    InputParameters inputParameters) {
        return new ClusteringMetricCompute(countServiceParameters, restTemplate, inputParameters.getAssemblyAccession(),
                                           inputParameters.getProjects());
    }
}
