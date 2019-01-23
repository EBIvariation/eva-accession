package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.FORCE_IMPORT_DECIDER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_FLOW;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.VALIDATE_CONTIGS_STEP;

@Configuration
public class ImportFlowConfiguration {

    @Autowired
    @Qualifier(IMPORT_DBSNP_VARIANTS_STEP)
    private Step importDbsnpVariantsStep;

    @Autowired
    @Qualifier(VALIDATE_CONTIGS_STEP)
    private Step validateContigsStep;

    @Autowired
    @Qualifier(FORCE_IMPORT_DECIDER)
    private JobExecutionDecider decider;

    @Bean(IMPORT_FLOW)
    public Flow optionalFlow() {
        return new FlowBuilder<Flow>("OPTIONAL_FLOW")
                .start(decider).on("TRUE")
                .to(importDbsnpVariantsStep)
                .from(decider).on("FALSE")
                .to(validateContigsStep)
                .next(importDbsnpVariantsStep)
                .build();
    }
}
