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

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.QC_SUBSNP_ACCESSION_JOB;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.QC_SUBSNP_ACCESSION_STEP;

@Configuration
@EnableBatchProcessing
public class QCSubsnpAccessionsJobConfiguration {
    @Autowired
    @Qualifier(QC_SUBSNP_ACCESSION_STEP)
    private Step qcSubsnpAccessionStep;

    @Bean(QC_SUBSNP_ACCESSION_JOB)
    public Job qcSubsnpAccessionJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(QC_SUBSNP_ACCESSION_JOB)
                .incrementer(new RunIdIncrementer())
                .start(qcSubsnpAccessionStep)
                .build();
    }
}
