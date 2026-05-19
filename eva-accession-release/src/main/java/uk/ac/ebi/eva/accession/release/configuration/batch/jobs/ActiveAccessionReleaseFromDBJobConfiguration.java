package uk.ac.ebi.eva.accession.release.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACTIVE_ACCESSIONS_RELEASE_FROM_DB_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACTIVE_ACCESSIONS_RELEASE_FROM_DB_STEP;

@Configuration
@EnableBatchProcessing
public class ActiveAccessionReleaseFromDBJobConfiguration {

    @Autowired
    @Qualifier(ACTIVE_ACCESSIONS_RELEASE_FROM_DB_STEP)
    private Step activeAccessionsReleaseFromDBStep;

    @Bean(ACTIVE_ACCESSIONS_RELEASE_FROM_DB_JOB)
    public Job activeAccessionReleaseFromDBJob(JobRepository jobRepository) {
        return new JobBuilder(ACTIVE_ACCESSIONS_RELEASE_FROM_DB_JOB, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(activeAccessionsReleaseFromDBStep)
                .build();
    }
}
