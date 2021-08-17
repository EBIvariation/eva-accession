package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringProgressListener;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {
    @Value("${eva.count-stats.url}")
    private String COUNT_STATS_BASE_URL;

    @Value("${eva.count-stats.username}")
    private String COUNT_STATS_USERNAME;

    @Value("${eva.count-stats.password}")
    private String COUNT_STATS_PASSWORD;

    @Bean(PROGRESS_LISTENER)
    public ClusteringProgressListener clusteringProgressListener(InputParameters parameters,
                                                                 ClusteringCounts clusteringCounts, RestTemplate restTemplate) {
        return new ClusteringProgressListener(parameters, clusteringCounts, restTemplate, COUNT_STATS_BASE_URL);
    }

    @Bean
    public ClusteringCounts importCounts() {
        return new ClusteringCounts();
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder) {
        return builder.basicAuthentication(COUNT_STATS_USERNAME, COUNT_STATS_PASSWORD).build();
    }
}
