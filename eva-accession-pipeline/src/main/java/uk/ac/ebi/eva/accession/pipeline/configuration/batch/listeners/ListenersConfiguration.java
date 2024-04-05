package uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.pipeline.batch.listeners.SubsnpAccessionsStepListener;
import uk.ac.ebi.eva.accession.pipeline.metric.AccessioningMetricCompute;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.metrics.configuration.MetricConfiguration;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SUBSNP_ACCESSION_STEP_LISTENER;

@Configuration
@Import({MetricConfiguration.class})
public class ListenersConfiguration {
    @Bean(SUBSNP_ACCESSION_STEP_LISTENER)
    public SubsnpAccessionsStepListener clusteringProgressListener(InputParameters parameters, MetricCompute metricCompute,
                                                                   SubmittedVariantAccessioningService submittedVariantAccessioningService) {
        return new SubsnpAccessionsStepListener(parameters, metricCompute, submittedVariantAccessioningService);
    }

    @Bean
    public MetricCompute getClusteringMetricCompute(CountServiceParameters countServiceParameters,
                                                    @Qualifier("COUNT_STATS_REST_TEMPLATE") RestTemplate restTemplate,
                                                    InputParameters inputParameters) {
        return new AccessioningMetricCompute(countServiceParameters, restTemplate, inputParameters.getAssemblyAccession(),
                inputParameters.getProjectAccession());
    }
}
