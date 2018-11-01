package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

@Configuration
public class InputParametersConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "release")
    public InputParameters inputParameters() {
        return new InputParameters();
    }
}
