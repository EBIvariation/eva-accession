package uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.DUPLICATE_SS_ACC_QC_JOB;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.DUPLICATE_SS_ACC_QC_STEP;

@Configuration
@EnableBatchProcessing
public class DuplicateSSAccQCJobConfiguration {

    @Autowired
    @Qualifier(DUPLICATE_SS_ACC_QC_STEP)
    private Step duplicateSSAccQCStep;

    @Bean(DUPLICATE_SS_ACC_QC_JOB)
    public Job duplicateSSAccQCJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(DUPLICATE_SS_ACC_QC_JOB)
                .incrementer(new RunIdIncrementer())
                .start(duplicateSSAccQCStep)
                .build();
    }
}
