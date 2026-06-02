package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.pipeline.batch.recovery.SSAccessionRecoveryService;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SS_ACCESSION_RECOVERY_SERVICE;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SS_ACCESSION_RECOVERY_STEP;

@Configuration
public class SSAccessionRecoveryStepConfiguration {
    @Autowired
    @Qualifier(SS_ACCESSION_RECOVERY_SERVICE)
    private SSAccessionRecoveryService SSAccessionRecoveryService;

    @Bean(SS_ACCESSION_RECOVERY_STEP)
    public Step ssAccessionRecoveryStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder(SS_ACCESSION_RECOVERY_STEP, jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    SSAccessionRecoveryService.runRecoveryForCategorySS();
                    return null;
                }, transactionManager)
                .build();
    }
}
