package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.recovery.RSAccessionRecoveryService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_SERVICE;

@Configuration
public class RSAccessionRecoveryJobListenerConfiguration {
    @Bean(RS_ACCESSION_RECOVERY_JOB_LISTENER)
    public JobExecutionListener jobExecutionListener(@Qualifier(RS_ACCESSION_RECOVERY_SERVICE)
                                                     RSAccessionRecoveryService RSAccessionRecoveryService) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                RSAccessionRecoveryService.setJobExecution(jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
            }
        };
    }
}
