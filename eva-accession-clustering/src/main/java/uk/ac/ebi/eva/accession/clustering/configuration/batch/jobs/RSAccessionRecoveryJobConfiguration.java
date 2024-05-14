package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs;

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

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_STEP;

@Configuration
@EnableBatchProcessing
public class RSAccessionRecoveryJobConfiguration {

    @Autowired
    @Qualifier(RS_ACCESSION_RECOVERY_STEP)
    private Step monotonicAccessionRecoveryAgentCategoryRSStep;

    @Autowired
    @Qualifier(RS_ACCESSION_RECOVERY_JOB_LISTENER)
    private JobExecutionListener monotonicAccessionRecoveryAgentCategoryRSJobListener;

    @Bean(RS_ACCESSION_RECOVERY_JOB)
    public Job createMonotonicAccessionRecoveryAgentCategoryRSJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(RS_ACCESSION_RECOVERY_JOB)
                .incrementer(new RunIdIncrementer())
                .start(monotonicAccessionRecoveryAgentCategoryRSStep)
                .listener(monotonicAccessionRecoveryAgentCategoryRSJobListener)
                .build();
    }

}
