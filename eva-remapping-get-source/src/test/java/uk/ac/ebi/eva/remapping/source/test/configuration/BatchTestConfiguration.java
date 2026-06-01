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

package uk.ac.ebi.eva.remapping.source.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;
import uk.ac.ebi.eva.remapping.source.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.remapping.source.configuration.batch.jobs.ExportSubmittedVariantsJobConfiguration;
import uk.ac.ebi.eva.remapping.source.configuration.batch.policies.PoliciesConfiguration;
import uk.ac.ebi.eva.remapping.source.runner.AccessionRemappingJobLauncherCommandLineRunner;

import static uk.ac.ebi.eva.remapping.source.configuration.BeanNames.EXPORT_SUBMITTED_VARIANTS_JOB;

@EnableAutoConfiguration
@Import({MongoConfiguration.class,
        InputParametersConfiguration.class,
        ExportSubmittedVariantsJobConfiguration.class,
        PoliciesConfiguration.class,
        AccessionRemappingJobLauncherCommandLineRunner.class
})
public class BatchTestConfiguration {

    public static final String JOB_EXPORT_SUBMITTED_VARIANTS_JOB = "JOB_EXPORT_SUBMITTED_VARIANTS_JOB";

    @Bean(JOB_EXPORT_SUBMITTED_VARIANTS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsFromMongo(JobLauncher jobLauncher, JobRepository jobRepository,
                                                              @Qualifier(EXPORT_SUBMITTED_VARIANTS_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }
}
