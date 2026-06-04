/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ResourceLoader;
import uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;
import uk.ac.ebi.eva.remapping.ingest.configuration.RemappingMetadataConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.batch.io.IngestRemappedSubmittedVariantsWriterConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.batch.io.VcfReaderConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.batch.jobs.IngestRemappedVariantsFromVcfJobConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.batch.listeners.ListenerConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.batch.processors.VariantProcessorConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.batch.steps.IngestRemappedFromVcfStepConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.batch.steps.StoreRemappingMetadataStepConfiguration;
import uk.ac.ebi.eva.remapping.ingest.configuration.policies.ChunkSizeCompletionPolicyConfiguration;

import static uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames.INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB;

@Import({InMemoryBatchConfiguration.class,
        IngestRemappedFromVcfStepConfiguration.class,
        StoreRemappingMetadataStepConfiguration.class,
        IngestRemappedVariantsFromVcfJobConfiguration.class,
        VcfReaderConfiguration.class,
        VariantProcessorConfiguration.class,
        IngestRemappedSubmittedVariantsWriterConfiguration.class,
        ListenerConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        RemappingMetadataConfiguration.class})
public class BatchTestConfiguration {
    public static final String JOB_INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB = "JOB_INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB";

    @Autowired
    private ResourceLoader resourceLoader;

    @Bean(JOB_INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsFromMongo(JobLauncher jobLauncher, JobRepository jobRepository,
                                                              @Qualifier(INGEST_REMAPPED_VARIANTS_FROM_VCF_JOB) Job job) {
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
