package uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.pipeline.batch.listeners.AccessioningProgressListener;
import uk.ac.ebi.eva.accession.pipeline.metric.AccessioningMetricComputeImpl;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.metrics.configuration.MetricConfiguration;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
@Import({MetricConfiguration.class})
public class ListenersConfiguration {
    @Bean(PROGRESS_LISTENER)
    public AccessioningProgressListener clusteringProgressListener(InputParameters parameters, MetricCompute metricCompute) {
        return new AccessioningProgressListener(parameters, metricCompute);
    }

    @Bean
    public MetricCompute getClusteringMetricCompute(CountServiceParameters countServiceParameters, RestTemplate restTemplate,
                                                    InputParameters inputParameters) {
        return new AccessioningMetricComputeImpl(countServiceParameters, restTemplate, inputParameters.getAssemblyAccession(),
                inputParameters.getProjectAccession());
    }
}
