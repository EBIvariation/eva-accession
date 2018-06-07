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
 *
 */
package uk.ac.ebi.eva.accession.pipeline.configuration.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_JOB;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;

@Configuration
@EnableBatchProcessing
public class CreateSubsnpAccessionsJobConfiguration {

    @Autowired
    @Qualifier(CREATE_SUBSNP_ACCESSION_STEP)
    private Step createSubsnpAccessionStep;

    @Autowired
    @Qualifier(CHECK_SUBSNP_ACCESSION_STEP)
    private Step checkSubsnpAccessionStep;

    @Bean(CREATE_SUBSNP_ACCESSION_JOB)
    public Job createSubsnpAccessionJob(JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(CREATE_SUBSNP_ACCESSION_JOB)
                                .incrementer(new RunIdIncrementer())
                                .flow(createSubsnpAccessionStep)
                                .next(checkSubsnpAccessionStep)
                                .end()
                                .build();
    }
}
