package uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners;

import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.pipeline.batch.io.AccessionWriter;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.ACCESSION_WRITER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SUBSNP_ACCESSION_JOB_LISTENER;

@Configuration
public class SubsnpAccessionJobExecutionListener {

    @Bean(SUBSNP_ACCESSION_JOB_LISTENER)
    public JobExecutionListener jobExecutionListener(@Qualifier(ACCESSION_WRITER) AccessionWriter accessionWriter,
                                                     SubmittedVariantAccessioningService submittedVariantAccessioningService) {
        return new JobExecutionListener() {
            @Override
            public void beforeJob(JobExecution jobExecution) {
                accessionWriter.setJobExecution(jobExecution);
            }

            @Override
            public void afterJob(JobExecution jobExecution) {
                submittedVariantAccessioningService.shutDownAccessionGenerator();
            }
        };
    }
}
