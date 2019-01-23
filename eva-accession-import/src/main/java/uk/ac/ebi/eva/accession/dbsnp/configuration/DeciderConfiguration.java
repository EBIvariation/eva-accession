package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.dbsnp.deciders.ForceImportDecider;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.FORCE_IMPORT_DECIDER;

@Configuration
public class DeciderConfiguration {

    @Bean(FORCE_IMPORT_DECIDER)
    public JobExecutionDecider decider() { return new ForceImportDecider(); }
}
