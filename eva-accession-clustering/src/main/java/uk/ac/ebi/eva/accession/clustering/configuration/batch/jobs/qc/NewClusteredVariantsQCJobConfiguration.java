/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.LineMapper;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.clustering.batch.io.qc.MissingCveReporter;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.RSHashPair;

import java.io.File;

@Configuration
@EnableBatchProcessing
// Since this is a QC Job, the configuration is intentionally lightweight and all collected in one-place
// to reduce overhead
public class NewClusteredVariantsQCJobConfiguration {

    public static final String NEW_CLUSTERED_VARIANTS_QC_JOB = "NEW_CLUSTERED_VARIANTS_QC_JOB";

    public static final String REPORT_MISSING_CVE_STEP = "REPORT_MISSING_CVE_STEP";

    public static final String RS_REPORT_READER = "RS_REPORT_READER";

    public static final String MISSING_CVE_REPORTER = "MISSING_CVE_REPORTER";

    static class RSReportLineMapper implements LineMapper<RSHashPair> {
        @Override
        public RSHashPair mapLine(String s, int i) {
            String[] tokens = s.split("\t");
            if (tokens.length >= 2) {
                return new RSHashPair(Long.parseLong(tokens[0]), tokens[1]);
            }
            throw new IllegalArgumentException("Invalid format for RS report");
        }
    }

    @Bean(RS_REPORT_READER)
    public ItemStreamReader<RSHashPair> rsReportReader(File rsReportFile) {
        FlatFileItemReader<RSHashPair> reader = new FlatFileItemReader<>();
        reader.setResource(new FileSystemResource(rsReportFile));
        reader.setLineMapper(new RSReportLineMapper());
        return reader;
    }

    @Bean(MISSING_CVE_REPORTER)
    public MissingCveReporter missingCveReporter(MongoTemplate mongoTemplate) {
        return new MissingCveReporter(mongoTemplate);
    }

    @Bean(REPORT_MISSING_CVE_STEP)
    public Step reportMissingCveStep(
            @Qualifier(RS_REPORT_READER) ItemStreamReader<RSHashPair> rsReportReader,
            @Qualifier(MISSING_CVE_REPORTER) ItemWriter<RSHashPair> missingCveReporter,
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        return stepBuilderFactory.get(REPORT_MISSING_CVE_STEP)
                                 .<RSHashPair, RSHashPair>chunk(chunkSizeCompletionPolicy)
                                 .reader(rsReportReader)
                                 .writer(missingCveReporter)
                                 .build();
    }

    @Bean(NEW_CLUSTERED_VARIANTS_QC_JOB)
    public Job ClusteringQCJob(
            @Qualifier(REPORT_MISSING_CVE_STEP) Step reportMissingCveStep,
            JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(NEW_CLUSTERED_VARIANTS_QC_JOB)
                .incrementer(new RunIdIncrementer())
                .start(reportMissingCveStep)
                .build();
    }
}
