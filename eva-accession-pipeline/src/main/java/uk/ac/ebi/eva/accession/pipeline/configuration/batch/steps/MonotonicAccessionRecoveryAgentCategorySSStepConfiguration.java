package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.pipeline.batch.recovery.MonotonicAccessionRecoveryAgentCategorySSService;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_SERVICE;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_STEP;

@Configuration
@EnableBatchProcessing
public class MonotonicAccessionRecoveryAgentCategorySSStepConfiguration {
    @Autowired
    @Qualifier(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_SERVICE)
    private MonotonicAccessionRecoveryAgentCategorySSService monotonicAccessionRecoveryAgentCategorySSService;

    @Bean(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_STEP)
    public Step monotonicAccessionRecoveryAgentCategorySSStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_STEP)
                .tasklet((contribution, chunkContext) -> {
                    monotonicAccessionRecoveryAgentCategorySSService.runRecoveryForCategorySS();
                    return null;
                })
                .build();
    }
}
