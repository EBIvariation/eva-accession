/*
 *
 *  * Copyright 2020 EMBL - European Bioinformatics Institute
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package uk.ac.ebi.eva.accession.clustering.test.configuration;

import org.springframework.batch.core.Job;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.ClusteringMongoReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.ClusteringWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.VcfReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.ClusteringFromMongoJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.ClusteringFromVcfJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.processors.ClusteringVariantProcessorConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.ClusteringFromMongoStepConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.ClusteringFromVcfStepConfiguration;

import javax.sql.DataSource;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_MONGO_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_VCF_JOB;

@EnableAutoConfiguration
@Import({ClusteringFromVcfJobConfiguration.class,
        ClusteringFromMongoJobConfiguration.class,
        ClusteringFromVcfStepConfiguration.class,
        ClusteringFromMongoStepConfiguration.class,
        VcfReaderConfiguration.class,
        ClusteringMongoReaderConfiguration.class,
        ClusteringVariantProcessorConfiguration.class,
        ClusteringWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        ListenersConfiguration.class})
public class BatchTestConfiguration {

    public static final String JOB_LAUNCHER_FROM_VCF = "JOB_LAUNCHER_FROM_VCF";

    public static final String JOB_LAUNCHER_FROM_MONGO = "JOB_LAUNCHER_FROM_MONGO";

    @Autowired
    private BatchProperties properties;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Bean(JOB_LAUNCHER_FROM_VCF)
    public JobLauncherTestUtils jobLauncherTestUtils() {

        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(CLUSTERING_FROM_VCF_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(JOB_LAUNCHER_FROM_MONGO)
    public JobLauncherTestUtils jobLauncherTestUtilsFromMongo() {

        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(CLUSTERING_FROM_MONGO_JOB) Job job) {
                super.setJob(job);
            }
        };
    }
}
