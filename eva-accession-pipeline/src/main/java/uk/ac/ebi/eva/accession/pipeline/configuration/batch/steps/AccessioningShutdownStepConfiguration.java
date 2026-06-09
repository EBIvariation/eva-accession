package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import static uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration.BATCH_TRANSACTION_MANAGER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;

@Configuration
public class AccessioningShutdownStepConfiguration {
    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Bean(ACCESSIONING_SHUTDOWN_STEP)
    public Step accessioningShutDownStep(JobRepository jobRepository,
                                         @Qualifier(BATCH_TRANSACTION_MANAGER) PlatformTransactionManager transactionManager) {
        return new StepBuilder(ACCESSIONING_SHUTDOWN_STEP, jobRepository)
                .tasklet((contribution, chunkContext) -> {
                    submittedVariantAccessioningService.shutDownAccessionGenerator();
                    return null;
                }, transactionManager)
                .build();
    }
}