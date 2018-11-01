package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.listeners.ExcludeVariantsListener;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EXCLUDE_VARIANTS_LISTENER;

@Configuration
public class ListenersConfiguration {

    @Bean(EXCLUDE_VARIANTS_LISTENER)
    public StepListenerSupport excludeVariantsListener() {
        return new ExcludeVariantsListener();
    }
}
