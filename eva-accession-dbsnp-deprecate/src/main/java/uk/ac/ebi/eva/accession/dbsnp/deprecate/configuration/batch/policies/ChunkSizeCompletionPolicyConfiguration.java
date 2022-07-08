package uk.ac.ebi.eva.accession.dbsnp.deprecate.configuration.batch.policies;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.dbsnp.deprecate.parameters.InputParameters;

@Configuration
public class ChunkSizeCompletionPolicyConfiguration {

    @Bean
    @StepScope
    public SimpleCompletionPolicy chunkSizecompletionPolicy(InputParameters inputParameters) {
        return new SimpleCompletionPolicy(inputParameters.getChunkSize());
    }
}
