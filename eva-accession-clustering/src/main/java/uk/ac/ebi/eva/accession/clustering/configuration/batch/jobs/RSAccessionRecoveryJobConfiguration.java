package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_JOB_LISTENER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_STEP;

@Configuration
public class RSAccessionRecoveryJobConfiguration {

    @Autowired
    @Qualifier(RS_ACCESSION_RECOVERY_STEP)
    private Step monotonicAccessionRecoveryAgentCategoryRSStep;

    @Autowired
    @Qualifier(RS_ACCESSION_RECOVERY_JOB_LISTENER)
    private JobExecutionListener monotonicAccessionRecoveryAgentCategoryRSJobListener;

    @Bean(RS_ACCESSION_RECOVERY_JOB)
    public Job createMonotonicAccessionRecoveryAgentCategoryRSJob(JobRepository jobRepository) {
        return new JobBuilder(RS_ACCESSION_RECOVERY_JOB, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(monotonicAccessionRecoveryAgentCategoryRSStep)
                .listener(monotonicAccessionRecoveryAgentCategoryRSJobListener)
                .build();
    }

}
