/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_TO_INSDC_PROCESSOR;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_WRITER;

/**
 * Creates a file with the contigs in INSDC (GenBank) when possible. The file will be used in
 * {@link CreateReleaseStepConfiguration} to include the contigs in the meta section of the VCF
 */
@Configuration
@EnableBatchProcessing
@Import({ContigReaderConfiguration.class,
        ContigToInsdcProcessorConfiguration.class,
        ContigWriterConfiguration.class})
public class ListContigsStepConfiguration {

    @Autowired
    @Qualifier(CONTIG_READER)
    private ItemStreamReader<String> contigReader;

    @Autowired
    @Qualifier(CONTIG_TO_INSDC_PROCESSOR)
    private ItemProcessor<String, String> contigProcessor;

    @Autowired
    @Qualifier(CONTIG_WRITER)
    private ItemStreamWriter<String> contigWriter;

    @Bean(LIST_CONTIGS_STEP)
    public Step contigsStep(StepBuilderFactory stepBuilderFactory, SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        TaskletStep step = stepBuilderFactory.get(LIST_CONTIGS_STEP)
                .<String, String>chunk(chunkSizeCompletionPolicy)
                .reader(contigReader)
                .processor(contigProcessor)
                .writer(contigWriter)
                .build();
        return step;
    }
}
