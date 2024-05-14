package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.pipeline.batch.recovery.SSAccessionRecoveryService;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SS_ACCESSION_RECOVERY_SERVICE;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SS_ACCESSION_RECOVERY_STEP;

@Configuration
@EnableBatchProcessing
public class SSAccessionRecoveryStepConfiguration {
    @Autowired
    @Qualifier(SS_ACCESSION_RECOVERY_SERVICE)
    private SSAccessionRecoveryService SSAccessionRecoveryService;

    @Bean(SS_ACCESSION_RECOVERY_STEP)
    public Step ssAccessionRecoveryStep(StepBuilderFactory stepBuilderFactory) {
        return stepBuilderFactory.get(SS_ACCESSION_RECOVERY_STEP)
                .tasklet((contribution, chunkContext) -> {
                    SSAccessionRecoveryService.runRecoveryForCategorySS();
                    return null;
                })
                .build();
    }
}
