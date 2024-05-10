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

package uk.ac.ebi.eva.accession.pipeline.test;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.io.AccessionWriterConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs.MonotonicAccessionRecoveryAgentCategorySSJobConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners.MonotonicAccessionRecoveryAgentCategorySSJobListenerConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.processors.VariantProcessorConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.recovery.MonotonicAccessionRecoveryAgentCategorySSServiceConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps.MonotonicAccessionRecoveryAgentCategorySSStepConfiguration;
import uk.ac.ebi.eva.accession.pipeline.runner.EvaAccessionJobLauncherCommandLineRunner;
import uk.ac.ebi.eva.commons.batch.configuration.SpringBoot1CompatibilityConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@EnableAutoConfiguration
@Import({MonotonicAccessionRecoveryAgentCategorySSJobConfiguration.class,
        MonotonicAccessionRecoveryAgentCategorySSStepConfiguration.class,
        MonotonicAccessionRecoveryAgentCategorySSServiceConfiguration.class,
        MonotonicAccessionRecoveryAgentCategorySSJobListenerConfiguration.class,
        AccessionWriterConfiguration.class, VariantProcessorConfiguration.class,
        EvaAccessionJobLauncherCommandLineRunner.class})
public class RecoveryAgentCategorySSTestConfiguration {
    @Bean
    public BatchConfigurer configurer(DataSource dataSource, EntityManagerFactory entityManagerFactory)
            throws Exception {
        return SpringBoot1CompatibilityConfiguration.getSpringBoot1CompatibleBatchConfigurer(dataSource,
                entityManagerFactory);
    }

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils(BatchConfigurer configurer) throws Exception {
        JobLauncherTestUtils jobLauncherTestUtils = new JobLauncherTestUtils();
        jobLauncherTestUtils.setJobLauncher(configurer.getJobLauncher());
        jobLauncherTestUtils.setJobRepository(configurer.getJobRepository());
        return jobLauncherTestUtils;
    }

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }
}