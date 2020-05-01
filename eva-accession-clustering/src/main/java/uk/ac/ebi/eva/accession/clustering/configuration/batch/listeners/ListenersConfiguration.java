package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {

    @Bean(PROGRESS_LISTENER)
    public <R, W> StepListenerSupport<R, W> progressListener(InputParameters parameters) {
        return new GenericProgressListener<>(parameters.getChunkSize());
    }
}
