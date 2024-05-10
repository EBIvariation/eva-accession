package uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.pipeline.batch.recovery.MonotonicAccessionRecoveryAgentCategorySSService;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_SERVICE;

@Configuration
public class MonotonicAccessionRecoveryAgentCategorySSJobListenerConfiguration {
    @Bean(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_JOB_LISTENER)
    public JobExecutionListener jobExecutionListener(@Qualifier(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_SERVICE)
                                                     MonotonicAccessionRecoveryAgentCategorySSService monotonicAccessionRecoveryAgentCategorySSService) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                monotonicAccessionRecoveryAgentCategorySSService.setJobExecution(jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
            }
        };
    }
}
