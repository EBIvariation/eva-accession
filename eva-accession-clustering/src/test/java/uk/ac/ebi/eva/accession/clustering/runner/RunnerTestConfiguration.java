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
 *
 */
package uk.ac.ebi.eva.accession.clustering.runner;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepContribution;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.clustering.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

@Configuration
@EnableBatchProcessing
@EnableAutoConfiguration
@Import(InputParametersConfiguration.class)
@ComponentScan(basePackages = {"uk.ac.ebi.eva.accession.clustering.runner"})
public class RunnerTestConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(RunnerTestConfiguration.class);

    public static final String TEST_JOB_NAME = "job1";

    public static final String TEST_STEP_1_NAME = "step1";

    public static final String TEST_STEP_2_NAME = "step2";

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Bean
    public Job job() throws Exception {
        return jobBuilderFactory.get(TEST_JOB_NAME)
                .start(step1()).next(step2())
                .build();
    }

    private Step step1() {
        return getStep(TEST_STEP_1_NAME);
    }

    private Step step2() {
        return getStep(TEST_STEP_2_NAME);
    }

    private Step getStep(String name) {
        return stepBuilderFactory.get(name)
                .tasklet(new Tasklet() {
                    public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws InterruptedException {
                        logger.info("Executing step " + name);
                        return null;
                    }
                })
                .build();
    }

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }
}
