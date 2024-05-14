package uk.ac.ebi.eva.accession.clustering.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.recovery.RSAccessionRecoveryService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_SERVICE;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_STEP;

@Configuration
@EnableBatchProcessing
public class RSAccessionRecoveryStepConfiguration {
    @Autowired
    @Qualifier(RS_ACCESSION_RECOVERY_SERVICE)
    private RSAccessionRecoveryService RSAccessionRecoveryService;

    @Bean(RS_ACCESSION_RECOVERY_STEP)
    public Step monotonicAccessionRecoveryAgentCategoryRSStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get(RS_ACCESSION_RECOVERY_STEP)
                .tasklet((contribution, chunkContext) -> {
                    RSAccessionRecoveryService.runRecoveryForCategoryRS();
                    return null;
                })
                .build();
    }
}
