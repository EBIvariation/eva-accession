package uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_JOB;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_STEP;

@Configuration
@EnableBatchProcessing
public class MonotonicAccessionRecoveryAgentCategorySSJobConfiguration {

    @Autowired
    @Qualifier(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_STEP)
    private Step monotonicAccessionRecoveryAgentCategorySSStep;

    @Autowired
    @Qualifier(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_JOB_LISTENER)
    private JobExecutionListener monotonicAccessionRecoveryAgentCategorySSJobListener;

    @Bean(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_JOB)
    public Job createMonotonicAccessionRecoveryAgentCategorySSJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_SS_JOB)
                .incrementer(new RunIdIncrementer())
                .start(monotonicAccessionRecoveryAgentCategorySSStep)
                .listener(monotonicAccessionRecoveryAgentCategorySSJobListener)
                .build();
    }

}
