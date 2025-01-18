package uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.qc;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_FILE_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_PROCESSOR;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_WRITER;

@Configuration
@EnableBatchProcessing
public class DuplicateRSAccQCStepConfiguration {

    @Autowired
    @Qualifier(DUPLICATE_RS_ACC_QC_FILE_READER)
    private ItemStreamReader<List<Long>> duplicateRSAccFileReader;

    @Autowired
    @Qualifier(DUPLICATE_RS_ACC_QC_PROCESSOR)
    private ItemProcessor<List<Long>, List<Long>> duplicateRSAccQCProcessor;

    @Autowired
    @Qualifier(DUPLICATE_RS_ACC_QC_WRITER)
    private ItemWriter<List<Long>> duplicateRSAccQCWriter;

    @Bean(DUPLICATE_RS_ACC_QC_STEP)
    public Step duplicateRSAccQCStep(StepBuilderFactory stepBuilderFactory) {
        TaskletStep step = stepBuilderFactory.get(DUPLICATE_RS_ACC_QC_STEP)
                // hardcoded the chunk size as 1, as the reader takes care of accumulating
                // and sending the chunk size (defined in properties file) elements to the processor
                .<List<Long>, List<Long>>chunk(1)
                .reader(duplicateRSAccFileReader)
                .processor(duplicateRSAccQCProcessor)
                .writer(duplicateRSAccQCWriter)
                .build();
        return step;
    }
}
