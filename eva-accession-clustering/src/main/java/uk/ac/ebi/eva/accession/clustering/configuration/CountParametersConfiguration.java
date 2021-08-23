package uk.ac.ebi.eva.accession.clustering.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.clustering.parameters.CountParameters;

@Configuration
public class CountParametersConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "eva.count-stats")
    public CountParameters countParameters() {
        return new CountParameters();
    }

}
