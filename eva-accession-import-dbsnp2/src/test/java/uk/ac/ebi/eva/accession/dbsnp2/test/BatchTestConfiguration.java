/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.dbsnp2.test;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.flow.ImportDbsnpJsonFlowConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.io.ImportDbsnpJsonVariantsReaderConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.io.ImportDbsnpJsonVariantsWriterConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.jobs.ImportDbsnpJsonVariantsJobConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.processors.JsonNodeToClusteredVariantProcessorConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.batch.steps.ImportDbsnpJsonVariantsStepConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.runner.DbsnpJsonImportVariantsJobLauncherCommandLineRunner;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_JOB;

@Configuration
@EnableAutoConfiguration
@EnableBatchProcessing
@Import({ListenersConfiguration.class,
        MongoConfiguration.class,
        ImportDbsnpJsonVariantsJobConfiguration.class,
        ImportDbsnpJsonVariantsStepConfiguration.class,
        ImportDbsnpJsonVariantsReaderConfiguration.class,
        JsonNodeToClusteredVariantProcessorConfiguration.class,
        ImportDbsnpJsonVariantsWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        InputParametersConfiguration.class,
        ImportDbsnpJsonFlowConfiguration.class,
        DbsnpJsonImportVariantsJobLauncherCommandLineRunner.class})
public class BatchTestConfiguration {

    public static final String JOB_IMPORT_DBSNP_JSON_VARIANTS_JOB = "JOB_IMPORT_DBSNP_JSON_VARIANTS_JOB";

    @Bean(JOB_IMPORT_DBSNP_JSON_VARIANTS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsFromMongo(JobLauncher jobLauncher, JobRepository jobRepository,
                                                              @Qualifier(IMPORT_DBSNP_JSON_VARIANTS_JOB) Job job) {
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
