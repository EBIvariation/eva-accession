package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACCESSION_RELEASE_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CREATE_RELEASE_STEP;

@Configuration
@EnableBatchProcessing
public class AccessionReleaseJobConfiguration {

    @Autowired
    @Qualifier(CREATE_RELEASE_STEP)
    private Step createReleaseStep;

    @Bean(ACCESSION_RELEASE_JOB)
    public Job accessionReleaseJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(ACCESSION_RELEASE_JOB)
                                .incrementer(new RunIdIncrementer())
                                .start(createReleaseStep)
                                .build();
    }
}
