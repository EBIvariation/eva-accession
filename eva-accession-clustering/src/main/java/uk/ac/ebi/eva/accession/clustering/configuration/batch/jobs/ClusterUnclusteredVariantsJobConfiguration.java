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

import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.ACCESSIONING_SHUTDOWN_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTER_UNCLUSTERED_VARIANTS_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.JOB_EXECUTION_LISTENER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_MERGE_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_SPLIT_CANDIDATES_STEP;

@Configuration
@EnableBatchProcessing
public class ClusterUnclusteredVariantsJobConfiguration {
    // Should be run after split or merge candidates have been processed in the step @see PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB
    // In this step, proceed to cluster as-yet unclustered variants in a given assembly
    // This job should only be run in parallel across instances
    @Bean(CLUSTER_UNCLUSTERED_VARIANTS_JOB)
    public Job clusteringFromMongoJob(
            @Qualifier(PROCESS_RS_MERGE_CANDIDATES_STEP) Step processRSMergeCandidatesStep,
            @Qualifier(PROCESS_RS_SPLIT_CANDIDATES_STEP) Step processRSSplitCandidatesStep,
            @Qualifier(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP) Step clearRSMergeAndSplitCandidatesStep,
            @Qualifier(CLUSTERING_NON_CLUSTERED_VARIANTS_FROM_MONGO_STEP) Step clusteringNonClusteredVariantsFromMongoStep,
            @Qualifier(ACCESSIONING_SHUTDOWN_STEP) Step accessioningShutdownStep,
            @Qualifier(JOB_EXECUTION_LISTENER) JobExecutionListener jobExecutionListener,
            JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(CLUSTER_UNCLUSTERED_VARIANTS_JOB)
                                .incrementer(new RunIdIncrementer())
                                .start(processRSMergeCandidatesStep)
                                .next(processRSSplitCandidatesStep)
                                .next(clearRSMergeAndSplitCandidatesStep)
                                .next(clusteringNonClusteredVariantsFromMongoStep)
                                .next(accessioningShutdownStep)
                                .listener(jobExecutionListener)
                                .build();
    }
}
