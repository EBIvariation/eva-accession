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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs;

import htsjdk.samtools.util.StringUtil;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.StepExecution;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_RS_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_MONGO_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_MERGE_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_SPLIT_CANDIDATES_STEP;

@Configuration
@EnableBatchProcessing
public class ClusteringFromMongoJobConfiguration {
    private JobExecutionDecider isRemappedAssemblyPresent(InputParameters inputParameters) {
        return new JobExecutionDecider() {
            @Override
            public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
                String status = (!StringUtil.isBlank(inputParameters.getRemappedFrom())) ? "TRUE": "FALSE";
                return new FlowExecutionStatus(status);
            }
        };
    }

    private Step dummyStep(StepBuilderFactory stepBuilderFactory, String arg) {
        return stepBuilderFactory.get("step_" + arg).tasklet(new Tasklet() {
            @Override
            public RepeatStatus execute(StepContribution stepContribution, ChunkContext chunkContext) throws Exception {
                return RepeatStatus.FINISHED;
            }
        }).build();
    }

    @Bean(CLUSTERING_FROM_MONGO_JOB)
    public Job clusteringFromMongoJob(@Qualifier(CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP) Step clusteringClusteredVariantsFromMongoStep,
                                      @Qualifier(PROCESS_RS_MERGE_CANDIDATES_STEP) Step processRSMergeCandidatesStep,
                                      @Qualifier(PROCESS_RS_SPLIT_CANDIDATES_STEP) Step processRSSplitCandidatesStep,
                                      @Qualifier(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP) Step clearRSMergeAndSplitCandidatesStep,
                                      @Qualifier(CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP) Step clusteringNonClusteredVariantsFromMongoStep,
                                      @Qualifier(BACK_PROPAGATE_RS_STEP) Step backPropagateRSStep,
                                      StepBuilderFactory stepBuilderFactory,
                                      JobBuilderFactory jobBuilderFactory,
                                      InputParameters inputParameters) {
        JobExecutionDecider jobExecutionDecider = isRemappedAssemblyPresent(inputParameters);
        Step dummyStep = dummyStep(stepBuilderFactory, "dummyStep");
        return jobBuilderFactory.get(CLUSTERING_FROM_MONGO_JOB)
                .incrementer(new RunIdIncrementer())
                //We need the dummy step here because Spring won't conditionally start the first step
                .start(dummyStep)
                .next(jobExecutionDecider)
                    .on("TRUE")
                    .to(new FlowBuilder<SimpleFlow>("remappedAssemblyClusteringFlow")
                            .start(clusteringClusteredVariantsFromMongoStep)
                            .next(processRSMergeCandidatesStep)
                            .next(processRSSplitCandidatesStep)
                            .next(clearRSMergeAndSplitCandidatesStep)
                            .next(clusteringNonClusteredVariantsFromMongoStep)
                            .next(backPropagateRSStep).build())
                    .on("*").end()
                .from(jobExecutionDecider)
                    .on("FALSE")
                    .to(clusteringNonClusteredVariantsFromMongoStep)
                    .on("*").end()
                .end().build();
    }
}
