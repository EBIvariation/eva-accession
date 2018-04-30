package uk.ac.ebi.eva.accession.pipeline.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.pipeline.policies.SkipPolicyVerification;

@Configuration
public class SkipPolicyVerificationConfiguration {

    @Bean
    public SkipPolicyVerification skipPolicyVerification() {
        return new SkipPolicyVerification();
    }
}
