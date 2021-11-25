/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.ExtraneousRSReporter;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.PendingMergeSplitReporter;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.RSReader;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.SSReader;
import uk.ac.ebi.eva.accession.clustering.batch.processors.qc.ReportUnclusteredSSProcessor;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
@EnableBatchProcessing
// Since this is a QC Job, the configuration is intentionally lightweight and all collected in one-place
// to reduce overhead
public class ClusteringQCJobConfiguration {

    public static final String REPORT_UNCLUSTERED_SS_AND_PENDING_MERGES_AND_SPLITS_STEP = "REPORT_UNCLUSTERED_SS_AND_PENDING_MERGES_AND_SPLITS_STEP";
    public static final String CLUSTERING_QC_JOB = "CLUSTERING_QC_JOB";
    public static final String PENDING_MERGE_AND_SPLIT_REPORTER = "PENDING_MERGE_SPLIT_REPORTER";
    public static final String REMAPPED_SS_READER = "QC_REMAPPED_SS_READER";
    public static final String REPORT_UNCLUSTERED_SS_PROCESSOR = "REPORT_UNCLUSTERED_SS_PROCESSOR";
    public static final String REMAPPED_RS_READER = "REMAPPED_RS_READER";
    public static final String REPORT_EXTRANEOUS_RS_STEP = "REPORT_EXTRANEOUS_RS_STEP";
    public static final String EXTRANEOUS_RS_REPORTER = "EXTRANEOUS_RS_REPORTER";

    @Bean(REMAPPED_SS_READER)
    public SSReader remappedSSReader(MongoTemplate mongoTemplate, InputParameters parameters) {
        return new SSReader(mongoTemplate, parameters.getAssemblyAccession(), parameters.getChunkSize());
    }

    @Bean(REMAPPED_RS_READER)
    public RSReader remappedRSReader(MongoTemplate mongoTemplate, InputParameters parameters) {
        return new RSReader(mongoTemplate, parameters.getAssemblyAccession(), parameters.getChunkSize());
    }

    @Bean(REPORT_UNCLUSTERED_SS_PROCESSOR)
    public ReportUnclusteredSSProcessor reportUnclusteredSSProcessor(Long accessioningMonotonicInitSs) {
        return new ReportUnclusteredSSProcessor(accessioningMonotonicInitSs);
    }

    @Bean(PENDING_MERGE_AND_SPLIT_REPORTER)
    public PendingMergeSplitReporter pendingMergeSplitReporter(
            @Qualifier(CLUSTERED_CLUSTERING_WRITER) ClusteringWriter clusteringWriter,
            InputParameters parameters,
            MongoTemplate mongoTemplate) {
        return new PendingMergeSplitReporter(parameters.getAssemblyAccession(), clusteringWriter, mongoTemplate);
    }

    @Bean(EXTRANEOUS_RS_REPORTER)
    public ExtraneousRSReporter extraneousRSReporter(InputParameters parameters, MongoTemplate mongoTemplate) {
        return new ExtraneousRSReporter(parameters.getAssemblyAccession(), mongoTemplate);
    }

    // QC step that reports unclustered SS and any pending merges/splits in the clustered assembly
    @Bean(REPORT_UNCLUSTERED_SS_AND_PENDING_MERGES_AND_SPLITS_STEP)
    public Step reportUnclusteredSSAndPendingMergeSplitStep(
            @Qualifier(REMAPPED_SS_READER) ItemStreamReader<SubmittedVariantEntity> remappedSSReader,
            @Qualifier(REPORT_UNCLUSTERED_SS_PROCESSOR)
                    ItemProcessor<SubmittedVariantEntity, SubmittedVariantEntity> reportUnclusteredSSProcessor,
            @Qualifier(PENDING_MERGE_AND_SPLIT_REPORTER)
                    ItemWriter<SubmittedVariantEntity> pendingMergeSplitReporter,
            @Qualifier(PROGRESS_LISTENER) StepExecutionListener progressListener,
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        return stepBuilderFactory.get(REPORT_UNCLUSTERED_SS_AND_PENDING_MERGES_AND_SPLITS_STEP)
                .<SubmittedVariantEntity, SubmittedVariantEntity>chunk(chunkSizeCompletionPolicy)
                .reader(remappedSSReader)
                .processor(reportUnclusteredSSProcessor)
                .writer(pendingMergeSplitReporter)
                .listener(progressListener)
                .build();
    }

    // QC step that reports extraneous RS i.e., RS not assigned to any SS
    @Bean(REPORT_EXTRANEOUS_RS_STEP)
    public Step reportExtraneousRSStep(
            @Qualifier(REMAPPED_RS_READER) ItemStreamReader<ClusteredVariantEntity> remappedRSReader,
            @Qualifier(EXTRANEOUS_RS_REPORTER)
                    ItemWriter<ClusteredVariantEntity> extraneousRSReporter,
            @Qualifier(PROGRESS_LISTENER) StepExecutionListener progressListener,
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy) {
        return stepBuilderFactory.get(REPORT_EXTRANEOUS_RS_STEP)
                .<ClusteredVariantEntity, ClusteredVariantEntity>chunk(chunkSizeCompletionPolicy)
                .reader(remappedRSReader)
                .writer(extraneousRSReporter)
                .listener(progressListener)
                .build();
    }

    @Bean(CLUSTERING_QC_JOB)
    public Job ClusteringQCJob(
            @Qualifier(REPORT_UNCLUSTERED_SS_AND_PENDING_MERGES_AND_SPLITS_STEP)
                    Step reportUnclusteredSSAndPendingMergeSplitStep,
            @Qualifier(REPORT_EXTRANEOUS_RS_STEP)
                    Step reportExtraneousRSStep,
            JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(CLUSTERING_QC_JOB)
                .incrementer(new RunIdIncrementer())
                .start(reportUnclusteredSSAndPendingMergeSplitStep)
                .next(reportExtraneousRSStep)
                .build();
    }
}
