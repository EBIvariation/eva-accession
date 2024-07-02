/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.release.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.builder.FlowJobBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.CreateDeprecatedReleaseStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.CreateMergedDeprecatedReleaseStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.CreateMergedReleaseStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.CreateMultimapReleaseStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.CreateReleaseStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.ListContigsStepConfiguration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.*;

@Configuration
@EnableBatchProcessing
@Import({CreateMergedDeprecatedReleaseStepConfiguration.class})
public class MergedDeprecatedAccessionReleaseJobConfiguration {

    /**
     * Note that we are just concatenating the DBSNP and EVA steps one after the other. We could configure them to run
     * in parallel if we want to. See ParallelStatisticsAndAnnotationFlowConfiguration in eva-pipeline for an example
     * of asynchronous parallel flows.
     */
    @Bean(MERGED_DEPRECATED_ACCESSION_RELEASE_JOB)
    public Job accessionReleaseJob(JobBuilderFactory jobBuilderFactory,
                                   @Qualifier(MERGED_DEPRECATED_ACCESSION_RELEASE_DBSNP_FLOW) Flow dbsnpFlow,
                                   @Qualifier(MERGED_DEPRECATED_ACCESSION_RELEASE_EVA_FLOW) Flow evaFlow) {
        FlowBuilder<FlowJobBuilder> flowBuilder = jobBuilderFactory.get(MERGED_DEPRECATED_ACCESSION_RELEASE_JOB)
                                                                   .incrementer(new RunIdIncrementer())
                                                                   .start(dbsnpFlow)
                                                                   .next(evaFlow);
        FlowJobBuilder jobBuilder = flowBuilder.build();
        return jobBuilder.build();
    }

    @Bean(MERGED_DEPRECATED_ACCESSION_RELEASE_DBSNP_FLOW)
    public Flow dbsnpFlow(
            @Qualifier(RELEASE_DBSNP_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP) Step createMergedDeprecatedReleaseStep) {
        return new FlowBuilder<Flow>(DBSNP_FLOW)
                .start(createMergedDeprecatedReleaseStep)
                .build();
    }

    @Bean(MERGED_DEPRECATED_ACCESSION_RELEASE_EVA_FLOW)
    public Flow evaFlow(
            @Qualifier(RELEASE_EVA_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP) Step createMergedDeprecatedReleaseStep) {
        return new FlowBuilder<Flow>(EVA_FLOW)
                .start(createMergedDeprecatedReleaseStep)
                .build();
    }
}
