package uk.ac.ebi.eva.accession.pipeline.configuration;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.steps.processors.VariantProcessor;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.VARIANT_PROCESSOR;

/**
 * Configuration to inject a VariantProcessor as a bean.
 */
@Configuration
public class VariantProcessorConfiguration {

    @Bean(VARIANT_PROCESSOR)
    @StepScope
    public VariantProcessor variantProcessor(InputParameters inputParameters) {
        String assemblyAccession = inputParameters.getAssemblyAccession();
        int taxonomyAccession = inputParameters.getTaxonomyAccession();
        String projectAccession = inputParameters.getProjectAccession();

        return new VariantProcessor(assemblyAccession, taxonomyAccession, projectAccession);
    }
}
