package uk.ac.ebi.eva.accession.release.configuration.batch.jobs;


import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_STEP;

@Configuration
public class DumpRSAccessionsJobConfiguration {

    @Autowired
    @Qualifier(DUMP_ACTIVE_ACCESSIONS_STEP)
    private Step dumpActiveAccessionsStep;

    @Autowired
    @Qualifier(DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_STEP)
    private Step dumpMergedAndDeprecatedAccessionsStep;

    @Bean(DUMP_ACTIVE_ACCESSIONS_JOB)
    public Job dumpActiveAccessionJob(JobRepository jobRepository) {
        return new JobBuilder(DUMP_ACTIVE_ACCESSIONS_JOB, jobRepository)
                .start(dumpActiveAccessionsStep)
                .build();
    }

    @Bean(DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB)
    public Job dumpMergedAndDeprecatedAccessionJob(JobRepository jobRepository) {
        return new JobBuilder(DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB, jobRepository)
                .start(dumpMergedAndDeprecatedAccessionsStep)
                .build();
    }

}
