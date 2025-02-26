package uk.ac.ebi.eva.accession.release.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP;

@Configuration
@EnableBatchProcessing
public class DeprecatedAccessionReleaseFromDBJobConfiguration {

    @Autowired
    @Qualifier(DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_STEP)
    private Step deprecatedAccessionsReleaseFromDBStep;

    @Bean(DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_JOB)
    public Job deprecatedAccessionReleaseFromDBJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(DEPRECATED_ACCESSIONS_RELEASE_FROM_DB_JOB)
                .incrementer(new RunIdIncrementer())
                .start(deprecatedAccessionsReleaseFromDBStep)
                .build();
    }
}
