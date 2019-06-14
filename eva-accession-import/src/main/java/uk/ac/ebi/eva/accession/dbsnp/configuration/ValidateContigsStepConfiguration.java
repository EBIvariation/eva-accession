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
 *
 */
package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.CONTIG_PROCESSOR;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.CONTIG_READER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.VALIDATE_CONTIGS_PROGRESS_LISTENER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.VALIDATE_CONTIGS_STEP;

//TODO remove comment-outs below by 01/07/2019
//@Configuration
//@EnableBatchProcessing
public class ValidateContigsStepConfiguration {

    @Autowired
    @Qualifier(CONTIG_READER)
    private ItemReader<String> contigReader;

    @Autowired
    @Qualifier(CONTIG_PROCESSOR)
    private ItemProcessor<String, String> contigProcessor;

    @Autowired
    @Qualifier(VALIDATE_CONTIGS_PROGRESS_LISTENER)
    private StepExecutionListener progressListener;

    @Bean(VALIDATE_CONTIGS_STEP)
    public Step validateContigsStep(StepBuilderFactory stepBuilderFactory,
                                    SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        ItemWriter<String> noOperationWriter = ignoredContigs -> { };

        TaskletStep step = stepBuilderFactory.get(VALIDATE_CONTIGS_STEP)
                .<String, String>chunk(chunkSizeCompletionPolicy)
                .reader(contigReader)
                .processor(contigProcessor)
                .writer(noOperationWriter)
                .listener(progressListener)
                .build();
        return step;
    }
}
