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
package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobParametersBuilder;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

@Configuration
@Import({MongoConfiguration.class})
public class RunnerConfiguration {

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }

    @Bean
    public CommandLineRunner runJob(JobLauncher jobLauncher, ApplicationContext context,
                                    @Value("${spring.batch.job.names}") String jobName) {
        return args -> {
            Job job = context.getBean(jobName, Job.class);
            JobExecution execution = jobLauncher.run(job, new JobParametersBuilder()
                    .toJobParameters());

            if (!execution.getExitStatus().equals(ExitStatus.COMPLETED)) {
                throw new RuntimeException("Job failed with status: " + execution.getExitStatus());
            }
        };
    }
}