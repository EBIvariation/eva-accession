/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.accession.pipeline.utils;

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

import uk.ac.ebi.eva.accession.pipeline.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

@Configuration
@EnableBatchProcessing
@EnableAutoConfiguration
@Import(InputParametersConfiguration.class)
@ComponentScan(basePackages = {"uk.ac.ebi.eva.accession.pipeline.runner"})
public class RunnerTestConfiguration {

    @Autowired
    private JobBuilderFactory jobBuilderFactory;

    @Autowired
    private StepBuilderFactory stepBuilderFactory;

    @Autowired
    private InputParameters inputParameters;

    // I think we don't need the steps being beans
    @Bean
    public Step step1() {
        return getSleepingStep("step1", 10);
    }

    @Bean
    public Step step2() {
        return getSleepingStep("step2", 30);
    }

    private Step getSleepingStep(String name, int seconds) {
        return stepBuilderFactory.get(name)
                          .tasklet(new Tasklet() {
                              public RepeatStatus execute(StepContribution contribution, ChunkContext chunkContext) throws InterruptedException {
                                  System.out.println("SLEEPING " + seconds + " SECONDS");
                                  Thread.sleep(seconds * 1000);
                                  return null;
                              }
                          })
                          .build();
    }

    @Bean
    public Job job(Step step1, Step step2) throws Exception {
        return jobBuilderFactory.get("job1")
//                                .incrementer(new RunIdIncrementer())
                                .start(step1).next(step2)
                                .build();
    }

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }
}