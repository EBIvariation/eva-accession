package uk.ac.ebi.eva.accession.release.configuration;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.listeners.ExcludeVariantsListener;
import uk.ac.ebi.eva.accession.release.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EXCLUDE_VARIANTS_LISTENER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {

    @Bean(EXCLUDE_VARIANTS_LISTENER)
    public StepListenerSupport excludeVariantsListener() {
        return new ExcludeVariantsListener();
    }

    @Bean(PROGRESS_LISTENER)
    public StepListenerSupport<Variant, VariantContext> importDbsnpVariantsProgressListener(
            InputParameters parameters) {
        return new GenericProgressListener<>(parameters.getChunkSize());
    }
}
