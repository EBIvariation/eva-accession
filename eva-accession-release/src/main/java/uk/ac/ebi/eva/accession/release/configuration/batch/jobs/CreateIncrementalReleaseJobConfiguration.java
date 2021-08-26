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
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.release.configuration.batch.steps.*;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.*;

// This class reads SS accessions from an accession report and creates an incremental release document from that

@Configuration
@EnableBatchProcessing
@Import({CreateIncrementalReleaseStepConfiguration.class})
public class CreateIncrementalReleaseJobConfiguration {

    @Autowired
    @Qualifier(CREATE_INCREMENTAL_ACCESSION_RELEASE_STEP)
    private Step createIncrementalAccessionReleaseStep;

    /**
     * Note that we are just concatenating the DBSNP and EVA steps one after the other. We could configure them to run
     * in parallel if we want to. See ParallelStatisticsAndAnnotationFlowConfiguration in eva-pipeline for an example
     * of asynchronous parallel flows.
     */
    @Bean(CREATE_INCREMENTAL_ACCESSION_RELEASE_JOB)
    public Job accessionReleaseJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(CREATE_INCREMENTAL_ACCESSION_RELEASE_JOB)
                .incrementer(new RunIdIncrementer())
                .start(createIncrementalAccessionReleaseStep)
                .build();
    }}
