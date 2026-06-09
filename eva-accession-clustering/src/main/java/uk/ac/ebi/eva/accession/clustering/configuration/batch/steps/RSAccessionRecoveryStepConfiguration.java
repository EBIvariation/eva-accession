package uk.ac.ebi.eva.accession.clustering.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.clustering.batch.recovery.RSAccessionRecoveryService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_SERVICE;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_STEP;
import static uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration.BATCH_TRANSACTION_MANAGER;

@Configuration
public class RSAccessionRecoveryStepConfiguration {
    @Autowired
    @Qualifier(RS_ACCESSION_RECOVERY_SERVICE)
    private RSAccessionRecoveryService RSAccessionRecoveryService;

    @Bean(RS_ACCESSION_RECOVERY_STEP)
    public Step monotonicAccessionRecoveryAgentCategoryRSStep(JobRepository jobRepository,
                                                              @Qualifier(BATCH_TRANSACTION_MANAGER)
                                                              PlatformTransactionManager transactionManager) {
        return new StepBuilder(RS_ACCESSION_RECOVERY_STEP, jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    RSAccessionRecoveryService.runRecoveryForCategoryRS();
                    return null;
                }, transactionManager)
                .build();
    }
}
