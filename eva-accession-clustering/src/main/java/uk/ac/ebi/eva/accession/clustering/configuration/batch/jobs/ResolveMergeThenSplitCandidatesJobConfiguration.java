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
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RESOLVE_MERGE_THEN_SPLIT_CANDIDATE_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_MERGE_CANDIDATES_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_RS_SPLIT_CANDIDATES_STEP;

@Configuration
@EnableBatchProcessing
public class ResolveMergeThenSplitCandidatesJobConfiguration {
    // Should be run after split or merge candidates have been processed in the step @see PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB
    // This job should only be run in parallel across instances
    @Bean(RESOLVE_MERGE_THEN_SPLIT_CANDIDATE_JOB)
    public Job clusteringFromMongoJob(
            @Qualifier(PROCESS_RS_MERGE_CANDIDATES_STEP) Step processRSMergeCandidatesStep,
            @Qualifier(PROCESS_RS_SPLIT_CANDIDATES_STEP) Step processRSSplitCandidatesStep,
            @Qualifier(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES_STEP) Step clearRSMergeAndSplitCandidatesStep,
            JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(RESOLVE_MERGE_THEN_SPLIT_CANDIDATE_JOB)
                                .incrementer(new RunIdIncrementer())
                                .start(processRSMergeCandidatesStep)
                                .next(processRSSplitCandidatesStep)
                                .next(clearRSMergeAndSplitCandidatesStep)
                                .build();
    }
}
