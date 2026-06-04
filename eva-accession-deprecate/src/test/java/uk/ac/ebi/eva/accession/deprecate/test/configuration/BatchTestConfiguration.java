/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.io.StudySubmittedVariantsFileReaderConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.io.StudySubmittedVariantsReaderConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.io.SubmittedVariantDeprecationWriterConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.jobs.DeprecateStudySubmittedVariantsJobConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.jobs.DeprecateSubmittedVariantsFromFileJobConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.listeners.ListenerConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.steps.DeprecateStudySubmittedVariantsStepConfiguration;
import uk.ac.ebi.eva.accession.deprecate.configuration.batch.steps.DeprecateSubmittedVariantsFromFileStepConfiguration;

import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATE_STUDY_SUBMITTED_VARIANTS_JOB;
import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATE_SUBMITTED_VARIANTS_FROM_FILE_JOB;

@Import({InMemoryBatchConfiguration.class,
        MongoConfiguration.class,
        InputParametersConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        StudySubmittedVariantsReaderConfiguration.class,
        StudySubmittedVariantsFileReaderConfiguration.class,
        SubmittedVariantDeprecationWriterConfiguration.class,
        DeprecateStudySubmittedVariantsStepConfiguration.class,
        DeprecateSubmittedVariantsFromFileStepConfiguration.class,
        DeprecateStudySubmittedVariantsJobConfiguration.class,
        DeprecateSubmittedVariantsFromFileJobConfiguration.class,
        ListenerConfiguration.class
})
public class BatchTestConfiguration {
    public static final String JOB_LAUNCHER_FROM_MONGO = "JOB_LAUNCHER_FROM_MONGO";
    public static final String JOB_LAUNCHER_FROM_FILE = "JOB_LAUNCHER_FROM_FILE";

    @Bean(JOB_LAUNCHER_FROM_MONGO)
    public JobLauncherTestUtils jobLauncherTestUtilsFromMongo(JobLauncher jobLauncher, JobRepository jobRepository,
                                                              @Qualifier(DEPRECATE_STUDY_SUBMITTED_VARIANTS_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;
    }

    @Bean(JOB_LAUNCHER_FROM_FILE)
    public JobLauncherTestUtils jobLauncherTestUtilsFromFile(JobLauncher jobLauncher, JobRepository jobRepository,
                                                             @Qualifier(DEPRECATE_SUBMITTED_VARIANTS_FROM_FILE_JOB) Job job) {
        JobLauncherTestUtils utils = new JobLauncherTestUtils();
        utils.setJobLauncher(jobLauncher);
        utils.setJobRepository(jobRepository);
        utils.setJob(job);
        return utils;

    }

    @Bean(name = "COUNT_STATS_REST_TEMPLATE")
    public RestTemplate countStatsRestTemplate() {
        return new RestTemplate();
    }


}
