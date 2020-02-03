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

package uk.ac.ebi.eva.accession.dbsnp.test;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.test.configuration.DbsnpTestDataSource;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.jobs.ImportDbsnpVariantsJobConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.processors.ImportDbsnpVariantsProcessorConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.io.ImportDbsnpVariantsReaderConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.steps.ImportDbsnpVariantsStepConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.io.ImportDbsnpVariantsWriterConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.flow.ImportFlowConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.processors.ValidateContigsProcessorConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.io.ValidateContigsReaderConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.batch.steps.ValidateContigsStepConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.runner.DbsnpImportVariantsJobLauncherCommandLineRunner;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import javax.sql.DataSource;

@EnableAutoConfiguration
@Import({DbsnpTestDataSource.class,
        MongoConfiguration.class,
        ImportDbsnpVariantsJobConfiguration.class,
        ImportDbsnpVariantsStepConfiguration.class,
        ImportDbsnpVariantsReaderConfiguration.class,
        ImportDbsnpVariantsProcessorConfiguration.class,
        ImportDbsnpVariantsWriterConfiguration.class,
        ValidateContigsStepConfiguration.class,
        ValidateContigsReaderConfiguration.class,
        ValidateContigsProcessorConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        InputParametersConfiguration.class,
        ListenersConfiguration.class,
        ImportFlowConfiguration.class,
        DbsnpImportVariantsJobLauncherCommandLineRunner.class})
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
