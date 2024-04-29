package uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionWriter;
import uk.ac.ebi.eva.accession.pipeline.batch.listeners.SubsnpAccessionsStepListener;
import uk.ac.ebi.eva.accession.pipeline.metric.AccessioningMetricCompute;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.metrics.configuration.MetricConfiguration;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSION_WRITER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SUBSNP_ACCESSION_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SUBSNP_ACCESSION_STEP_LISTENER;

@Configuration
@Import({MetricConfiguration.class})
public class ListenersConfiguration {
    @Bean(SUBSNP_ACCESSION_STEP_LISTENER)
    public SubsnpAccessionsStepListener clusteringProgressListener(InputParameters parameters, MetricCompute metricCompute) {
        return new SubsnpAccessionsStepListener(parameters, metricCompute);
    }

    @Bean
    public MetricCompute getClusteringMetricCompute(CountServiceParameters countServiceParameters,
                                                    @Qualifier("COUNT_STATS_REST_TEMPLATE") RestTemplate restTemplate,
                                                    InputParameters inputParameters) {
        return new AccessioningMetricCompute(countServiceParameters, restTemplate, inputParameters.getAssemblyAccession(),
                inputParameters.getProjectAccession());
    }

    @Bean(SUBSNP_ACCESSION_JOB_LISTENER)
    public JobExecutionListener jobExecutionListener(@Qualifier(ACCESSION_WRITER) AccessionWriter accessionWriter) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                accessionWriter.setJobExecution(jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                // Do nothing
            }
        };
    }
}
