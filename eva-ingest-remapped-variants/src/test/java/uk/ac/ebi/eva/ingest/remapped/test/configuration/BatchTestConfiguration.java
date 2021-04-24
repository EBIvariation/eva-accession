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
package uk.ac.ebi.eva.ingest.remapped.test.configuration;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;

import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;
import uk.ac.ebi.eva.ingest.remapped.configuration.batch.io.IngestRemappedSubmittedVariantsWriterConfiguration;
import uk.ac.ebi.eva.ingest.remapped.configuration.batch.io.VcfReaderConfiguration;
import uk.ac.ebi.eva.ingest.remapped.configuration.batch.jobs.IngestRemappedVariantsFromVcfJobConfiguration;
import uk.ac.ebi.eva.ingest.remapped.configuration.batch.processors.VariantToSubmittedVariantEntityRemappedProcessorConfiguration;
import uk.ac.ebi.eva.ingest.remapped.configuration.batch.steps.IngestRemappedFromVcfStepConfiguration;
import uk.ac.ebi.eva.ingest.remapped.configuration.policies.ChunkSizeCompletionPolicyConfiguration;

import javax.sql.DataSource;

@EnableAutoConfiguration
@Import({IngestRemappedFromVcfStepConfiguration.class,
        IngestRemappedVariantsFromVcfJobConfiguration.class,
        VcfReaderConfiguration.class,
        VariantToSubmittedVariantEntityRemappedProcessorConfiguration.class,
        IngestRemappedSubmittedVariantsWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
public class BatchTestConfiguration {

    @Autowired
    private BatchProperties properties;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }

}
