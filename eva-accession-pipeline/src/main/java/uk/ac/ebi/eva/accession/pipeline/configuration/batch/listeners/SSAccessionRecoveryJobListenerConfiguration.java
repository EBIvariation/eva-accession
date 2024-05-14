package uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.pipeline.batch.recovery.SSAccessionRecoveryService;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SS_ACCESSION_RECOVERY_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SS_ACCESSION_RECOVERY_SERVICE;

@Configuration
public class SSAccessionRecoveryJobListenerConfiguration {
    @Bean(SS_ACCESSION_RECOVERY_JOB_LISTENER)
    public JobExecutionListener jobExecutionListener(@Qualifier(SS_ACCESSION_RECOVERY_SERVICE)
                                                     SSAccessionRecoveryService SSAccessionRecoveryService) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                SSAccessionRecoveryService.setJobExecution(jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
            }
        };
    }
}
