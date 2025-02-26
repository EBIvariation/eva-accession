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

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_DEPRECATED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_DEPRECATED_ACCESSIONS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_ACCESSIONS_STEP;

@Configuration
@EnableBatchProcessing
public class DumpRSAccessionsJobConfiguration {

    @Autowired
    @Qualifier(DUMP_ACTIVE_ACCESSIONS_STEP)
    private Step dumpActiveAccessionsStep;

    @Autowired
    @Qualifier(DUMP_MERGED_ACCESSIONS_STEP)
    private Step dumpMergedAccessionsStep;

    @Autowired
    @Qualifier(DUMP_DEPRECATED_ACCESSIONS_STEP)
    private Step dumpDeprecatedAccessionsStep;

    @Bean(DUMP_ACTIVE_ACCESSIONS_JOB)
    public Job dumpActiveAccessionJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(DUMP_ACTIVE_ACCESSIONS_JOB)
                .incrementer(new RunIdIncrementer())
                .start(dumpActiveAccessionsStep)
                .build();
    }

    @Bean(DUMP_MERGED_ACCESSIONS_JOB)
    public Job dumpMergedAccessionJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(DUMP_MERGED_ACCESSIONS_JOB)
                .incrementer(new RunIdIncrementer())
                .start(dumpMergedAccessionsStep)
                .build();
    }

    @Bean(DUMP_DEPRECATED_ACCESSIONS_JOB)
    public Job dumpDeprecatedAccessionJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(DUMP_DEPRECATED_ACCESSIONS_JOB)
                .incrementer(new RunIdIncrementer())
                .start(dumpDeprecatedAccessionsStep)
                .build();
    }

}
