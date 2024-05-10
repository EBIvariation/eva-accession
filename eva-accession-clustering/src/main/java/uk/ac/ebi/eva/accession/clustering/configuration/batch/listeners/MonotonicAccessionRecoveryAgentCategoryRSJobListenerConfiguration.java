package uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.recovery.MonotonicAccessionRecoveryAgentCategoryRSService;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_SERVICE;

@Configuration
public class MonotonicAccessionRecoveryAgentCategoryRSJobListenerConfiguration {
    @Bean(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_JOB_LISTENER)
    public JobExecutionListener jobExecutionListener(@Qualifier(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_SERVICE)
                                                     MonotonicAccessionRecoveryAgentCategoryRSService monotonicAccessionRecoveryAgentCategoryRSService) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                monotonicAccessionRecoveryAgentCategoryRSService.setJobExecution(jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
            }
        };
    }
}
