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

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.transaction.PlatformTransactionManager;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ImportDbsnpJsonFlowConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ImportDbsnpJsonVariantsJobConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ImportDbsnpJsonVariantsReaderConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ImportDbsnpJsonVariantsStepConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ImportDbsnpJsonVariantsWriterConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.JsonNodeToClusteredVariantProcessorConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.configuration.ListenersConfiguration;
import uk.ac.ebi.eva.accession.dbsnp2.runner.DbsnpJsonImportVariantsJobLauncherCommandLineRunner;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

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

    @Autowired
    private BatchProperties properties;

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
