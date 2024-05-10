package uk.ac.ebi.eva.accession.clustering.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.recovery.MonotonicAccessionRecoveryAgentCategoryRSService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_SERVICE;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_STEP;

@Configuration
@EnableBatchProcessing
public class MonotonicAccessionRecoveryAgentCategoryRSStepConfiguration {
    @Autowired
    @Qualifier(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_SERVICE)
    private MonotonicAccessionRecoveryAgentCategoryRSService monotonicAccessionRecoveryAgentCategoryRSService;

    @Bean(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_STEP)
    public Step monotonicAccessionRecoveryAgentCategoryRSStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_STEP)
                .tasklet((contribution, chunkContext) -> {
                    monotonicAccessionRecoveryAgentCategoryRSService.runRecoveryForCategoryRS();
                    return null;
                })
                .build();
    }
}
