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
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.CreateMergedReleaseStepConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.ListContigsStepConfiguration;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.*;

@Configuration
@EnableBatchProcessing
@Import({ListContigsStepConfiguration.class,
         CreateMergedReleaseStepConfiguration.class})
public class MergedAccessionReleaseJobConfiguration {

    @Bean(MERGED_ACCESSION_RELEASE_JOB)
    public Job accessionReleaseJob(JobBuilderFactory jobBuilderFactory,
                                   @Qualifier(MERGED_ACCESSION_RELEASE_DBSNP_FLOW) Flow dbsnpFlow,
                                   @Qualifier(MERGED_ACCESSION_RELEASE_EVA_FLOW)Flow evaFlow) {
        FlowBuilder<FlowJobBuilder> flowBuilder = jobBuilderFactory.get(MERGED_ACCESSION_RELEASE_JOB)
                                                                   .incrementer(new RunIdIncrementer())
                                                                   .start(dbsnpFlow)
                                                                   .next(evaFlow);
        FlowJobBuilder jobBuilder = flowBuilder.build();
        return jobBuilder.build();
    }

    @Bean(MERGED_ACCESSION_RELEASE_DBSNP_FLOW)
    public Flow dbsnpFlow(
            @Qualifier(LIST_DBSNP_MERGED_CONTIGS_STEP) Step listMergedContigsStep,
            @Qualifier(RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP) Step createMergedReleaseStep) {
        return new FlowBuilder<Flow>(DBSNP_FLOW)
                .start(listMergedContigsStep)
                .next(createMergedReleaseStep)
                .build();
    }

    @Bean(MERGED_ACCESSION_RELEASE_EVA_FLOW)
    public Flow evaFlow(
            @Qualifier(LIST_EVA_MERGED_CONTIGS_STEP) Step listMergedContigsStep,
            @Qualifier(RELEASE_EVA_MAPPED_MERGED_VARIANTS_STEP) Step createMergedReleaseStep) {
        return new FlowBuilder<Flow>(EVA_FLOW)
                .start(listMergedContigsStep)
                .next(createMergedReleaseStep)
                .build();
    }
}
