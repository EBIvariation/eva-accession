package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringProgressListener;
import uk.ac.ebi.eva.accession.clustering.parameters.CountParameters;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {

    @Bean(PROGRESS_LISTENER)
    public ClusteringProgressListener clusteringProgressListener(InputParameters parameters,
                                                                 ClusteringCounts clusteringCounts,
                                                                 CountParameters countParameters,
                                                                 RestTemplate restTemplate) {
        return new ClusteringProgressListener(parameters, clusteringCounts, restTemplate, countParameters.getUrl());
    }

    @Bean
    public ClusteringCounts importCounts() {
        return new ClusteringCounts();
    }

    @Bean
    public RestTemplate restTemplate(RestTemplateBuilder builder, CountParameters countParameters) {
        return builder.basicAuthentication(countParameters.getUserName(), countParameters.getPassword()).build();
    }
}
