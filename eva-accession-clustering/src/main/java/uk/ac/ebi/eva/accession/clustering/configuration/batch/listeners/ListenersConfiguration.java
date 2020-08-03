package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringProgressListener;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {

    @Bean(PROGRESS_LISTENER)
    public ClusteringProgressListener clusteringProgressListener(InputParameters parameters,
                                                                 ClusteringCounts clusteringCounts) {
        return new ClusteringProgressListener(parameters.getChunkSize(), clusteringCounts);
    }

    @Bean
    public ClusteringCounts importCounts() {
        return new ClusteringCounts();
    }
}
