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
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.RSAccessionRecoveryJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners.RSAccessionRecoveryJobListenerConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.recovery.RSAccessionRecoveryServiceConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.RSAccessionRecoveryStepConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_JOB;

@EnableAutoConfiguration
@Import({RSAccessionRecoveryJobConfiguration.class,
        RSAccessionRecoveryStepConfiguration.class,
        RSAccessionRecoveryServiceConfiguration.class,
        RSAccessionRecoveryJobListenerConfiguration.class,
        ClusteredVariantAccessioningConfiguration.class
})
public class RSAccessionRecoveryTestConfiguration {
    public static final String JOB_LAUNCHER_RS_ACCESSION_RECOVERY = "JOB_LAUNCHER_RS_ACCESSION_RECOVERY";

    @Bean(JOB_LAUNCHER_RS_ACCESSION_RECOVERY)
    public JobLauncherTestUtils jobLauncherTestUtilsMonotonicAccessionRecoveryAgent(JobLauncher jobLauncher,
                                                                                    JobRepository jobRepository,
                                                                                    @Qualifier(RS_ACCESSION_RECOVERY_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }
}