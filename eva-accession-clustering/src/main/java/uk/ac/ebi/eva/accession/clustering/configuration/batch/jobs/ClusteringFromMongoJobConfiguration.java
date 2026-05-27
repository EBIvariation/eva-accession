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
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.batch.core.job.flow.support.SimpleFlow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.NonNull;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_NEW_RS_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_SPLIT_OR_MERGED_RS_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_MONGO_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.JOB_EXECUTION_LISTENER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_MERGE_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_SPLIT_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration.BATCH_TRANSACTION_MANAGER;

@Configuration
public class ClusteringFromMongoJobConfiguration {
    private JobExecutionDecider isRemappedAssemblyPresent(InputParameters inputParameters) {
        return new JobExecutionDecider() {
            @Override
            @NonNull
            public FlowExecutionStatus decide(@NonNull JobExecution jobExecution, StepExecution stepExecution) {
                String status = (!StringUtil.isBlank(inputParameters.getRemappedFrom())) ? "TRUE" : "FALSE";
                return new FlowExecutionStatus(status);
            }
        };
    }

    private Step dummyStep(JobRepository jobRepository,
                           @Qualifier(BATCH_TRANSACTION_MANAGER) PlatformTransactionManager transactionManager) {
        return new StepBuilder("step_" + "dummyStep", jobRepository)
                .tasklet((stepContribution, chunkContext) -> RepeatStatus.FINISHED,
                        transactionManager)
                .build();
    }

    @Bean(CLUSTERING_FROM_MONGO_JOB)
    public Job clusteringFromMongoJob(@Qualifier(CLUSTERING_CLUSTERED_VARIANTS_FROM_MONGO_STEP) Step clusteringClusteredVariantsFromMongoStep,
                                      @Qualifier(PROCESS_RS_MERGE_CANDIDATES_STEP) Step processRSMergeCandidatesStep,
                                      @Qualifier(PROCESS_RS_SPLIT_CANDIDATES_STEP) Step processRSSplitCandidatesStep,
                                      @Qualifier(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP) Step clearRSMergeAndSplitCandidatesStep,
                                      @Qualifier(CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP) Step clusteringNonClusteredVariantsFromMongoStep,
                                      @Qualifier(ACCESSIONING_SHUTDOWN_STEP) Step accessioningShutdownStep,
                                      // Back-propagate RS that were newly created in the remapped assembly
                                      @Qualifier(BACK_PROPAGATE_NEW_RS_STEP) Step backPropagateNewRSStep,
                                      // Back-propagate RS in the remapped assembly that were split or merged
                                      @Qualifier(BACK_PROPAGATE_SPLIT_OR_MERGED_RS_STEP)
                                      Step backPropagateSplitMergedRSStep,
                                      @Qualifier(JOB_EXECUTION_LISTENER) JobExecutionListener jobExecutionListener,
                                      JobRepository jobRepository,
                                      @Qualifier(BATCH_TRANSACTION_MANAGER) PlatformTransactionManager transactionManager,
                                      InputParameters inputParameters) {
        JobExecutionDecider jobExecutionDecider = isRemappedAssemblyPresent(inputParameters);
        Step dummyStep = dummyStep(jobRepository, transactionManager);
        return new JobBuilder(CLUSTERING_FROM_MONGO_JOB, jobRepository)
                .incrementer(new RunIdIncrementer())
                //We need the dummy step here because Spring won't conditionally start the first step
                .start(dummyStep)
                .listener(jobExecutionListener)
                .next(jobExecutionDecider)
                .on("TRUE")
                .to(new FlowBuilder<SimpleFlow>("remappedAssemblyClusteringFlow")
                        .start(clusteringClusteredVariantsFromMongoStep)
                        .next(processRSMergeCandidatesStep)
                        .next(processRSSplitCandidatesStep)
                        .next(clearRSMergeAndSplitCandidatesStep)
                        .next(clusteringNonClusteredVariantsFromMongoStep)
                        .next(accessioningShutdownStep)
                        .next(backPropagateNewRSStep)
                        .next(backPropagateSplitMergedRSStep)
                        .build())
                .on("*").end()
                .from(jobExecutionDecider)
                .on("FALSE")
                .to(clusteringNonClusteredVariantsFromMongoStep)
                .next(accessioningShutdownStep)
                .on("*").end()
                .end().build();
    }
}
