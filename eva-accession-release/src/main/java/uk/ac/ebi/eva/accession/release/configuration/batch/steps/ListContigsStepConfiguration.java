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
package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.release.configuration.batch.io.ContigReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.ContigWriterConfiguration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_ACTIVE_CONTIG_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_ACTIVE_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_ACTIVE_CONTIG_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_ACTIVE_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_CONTIG_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_MERGED_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_DBSNP_ACTIVE_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_DBSNP_MERGED_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_CONTIG_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_MERGED_CONTIG_WRITER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_EVA_ACTIVE_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_EVA_MERGED_CONTIGS_STEP;

/**
 * Creates a file with the contigs in INSDC (GenBank) when possible. The file will be used in
 * {@link CreateReleaseStepConfiguration} to include the contigs in the meta section of the VCF
 */
@Configuration
@EnableBatchProcessing
@Import({ContigReaderConfiguration.class,
        ContigWriterConfiguration.class})
public class ListContigsStepConfiguration {

    @Bean(LIST_DBSNP_ACTIVE_CONTIGS_STEP)
    public Step activeContigsStepDbsnp(
            StepBuilderFactory stepBuilderFactory, SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(DBSNP_ACTIVE_CONTIG_READER) ItemStreamReader<String> activeContigReader,
            @Qualifier(DBSNP_ACTIVE_CONTIG_WRITER) ItemStreamWriter<String> activeContigWriter) {
        TaskletStep step = stepBuilderFactory.get(LIST_DBSNP_ACTIVE_CONTIGS_STEP)
                .<String, String>chunk(chunkSizeCompletionPolicy)
                .reader(activeContigReader)
                .writer(activeContigWriter)
                .build();
        return step;
    }

    @Bean(LIST_DBSNP_MERGED_CONTIGS_STEP)
    public Step mergedContigsStepDbsnp(
            StepBuilderFactory stepBuilderFactory, SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(DBSNP_MERGED_CONTIG_READER) ItemStreamReader<String> mergedContigReader,
            @Qualifier(DBSNP_MERGED_CONTIG_WRITER) ItemStreamWriter<String> mergedContigWriter) {
        TaskletStep step = stepBuilderFactory.get(LIST_DBSNP_MERGED_CONTIGS_STEP)
                .<String, String>chunk(chunkSizeCompletionPolicy)
                .reader(mergedContigReader)
                .writer(mergedContigWriter)
                .build();
        return step;
    }

    @Bean(LIST_EVA_ACTIVE_CONTIGS_STEP)
    public Step activeContigsStepEva(
            StepBuilderFactory stepBuilderFactory, SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(EVA_ACTIVE_CONTIG_READER) ItemStreamReader<String> activeContigReader,
            @Qualifier(EVA_ACTIVE_CONTIG_WRITER) ItemStreamWriter<String> activeContigWriter) {
        TaskletStep step = stepBuilderFactory.get(LIST_EVA_ACTIVE_CONTIGS_STEP)
                .<String, String>chunk(chunkSizeCompletionPolicy)
                .reader(activeContigReader)
                .writer(activeContigWriter)
                .build();
        return step;
    }

    @Bean(LIST_EVA_MERGED_CONTIGS_STEP)
    public Step mergedContigsStepEva(
            StepBuilderFactory stepBuilderFactory, SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(EVA_MERGED_CONTIG_READER) ItemStreamReader<String> mergedContigReader,
            @Qualifier(EVA_MERGED_CONTIG_WRITER) ItemStreamWriter<String> mergedContigWriter) {
        TaskletStep step = stepBuilderFactory.get(LIST_EVA_MERGED_CONTIGS_STEP)
                .<String, String>chunk(chunkSizeCompletionPolicy)
                .reader(mergedContigReader)
                .writer(mergedContigWriter)
                .build();
        return step;
    }
}
