package uk.ac.ebi.eva.accession.deprecate.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.deprecate.parameters.InputParameters;

@Configuration
public class InputParametersConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "parameters")
    public InputParameters inputParameters() {
        return new InputParameters();
    }
}
