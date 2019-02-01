package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_PROCESSOR;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_WRITER;

@Configuration
@EnableBatchProcessing
public class ContigStepConfiguration {

    @Autowired
    @Qualifier(CONTIG_READER)
    private JdbcCursorItemReader<String> contigReader;

    @Autowired
    @Qualifier(CONTIG_PROCESSOR)
    private ItemProcessor<String, String> contigProcessor;

    @Autowired
    @Qualifier(CONTIG_WRITER)
    private ItemStreamWriter<String> contigWriter;

    @Bean(CONTIG_STEP)
    public Step contigsStep(StepBuilderFactory stepBuilderFactory, SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(CONTIG_STEP)
                .<String, String>chunk(chunkSizeCompletionPolicy)
                .reader(contigReader)
                .processor(contigProcessor)
                .writer(contigWriter)
                .build();
        return step;
    }
}
