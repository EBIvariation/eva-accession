package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc;

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

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_STEP;

@Configuration
@EnableBatchProcessing
public class DuplicateRSAccQCJobConfiguration {

    @Autowired
    @Qualifier(DUPLICATE_RS_ACC_QC_STEP)
    private Step duplicateRSAccQCStep;

    @Bean(DUPLICATE_RS_ACC_QC_JOB)
    public Job duplicateRSAccQCJob(JobRepository jobRepository) {
        return new JobBuilder(DUPLICATE_RS_ACC_QC_JOB, jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(duplicateRSAccQCStep)
                .build();
    }
}
