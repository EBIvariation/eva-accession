package uk.ac.ebi.eva.accession.pipeline.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

@Configuration
public class RunnerConfiguration {

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }
}