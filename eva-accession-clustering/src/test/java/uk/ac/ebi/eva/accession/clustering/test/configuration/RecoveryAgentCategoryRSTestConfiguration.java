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

package uk.ac.ebi.eva.accession.clustering.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.MonotonicAccessionRecoveryAgentCategoryRSJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners.MonotonicAccessionRecoveryAgentCategoryRSJobListenerConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.recovery.MonotonicAccessionRecoveryAgentCategoryRSServiceConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.MonotonicAccessionRecoveryAgentCategoryRSStepConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.commons.batch.configuration.SpringBoot1CompatibilityConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_JOB;

@EnableAutoConfiguration
@Import({MonotonicAccessionRecoveryAgentCategoryRSJobConfiguration.class,
        MonotonicAccessionRecoveryAgentCategoryRSStepConfiguration.class,
        MonotonicAccessionRecoveryAgentCategoryRSServiceConfiguration.class,
        MonotonicAccessionRecoveryAgentCategoryRSJobListenerConfiguration.class,
        ClusteredVariantAccessioningConfiguration.class
})
public class RecoveryAgentCategoryRSTestConfiguration {
    public static final String JOB_LAUNCHER_MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS = "JOB_LAUNCHER_MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS";

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

    @Bean(JOB_LAUNCHER_MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS)
    public JobLauncherTestUtils jobLauncherTestUtilsMonotonicAccessionRecoveryAgent() {

        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(MONOTONIC_ACCESSION_RECOVERY_AGENT_CATEGORY_RS_JOB) Job job) {
                super.setJob(job);
            }
        };
    }
}