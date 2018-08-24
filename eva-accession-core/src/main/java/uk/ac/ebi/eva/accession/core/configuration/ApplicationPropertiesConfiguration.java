package uk.ac.ebi.eva.accession.core.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class ApplicationPropertiesConfiguration {

    @Bean
    @ConfigurationProperties(prefix = "accessioning")
    public ApplicationProperties applicationProperties() {
        return new ApplicationProperties();
    }

}
