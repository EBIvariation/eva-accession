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
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.BackPropagatedRSWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.ClusteringMongoReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.ClusteringWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.DuplicateRSAccQCFileReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.DuplicateRSAccQCProcessorConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.DuplicateRSAccQCWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.TargetSSReaderForBackPropRSConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.VcfReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.BackPropagateRSJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.ClusterUnclusteredVariantsJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.ClusteringFromMongoJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.ClusteringFromVcfJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.ProcessRemappedVariantsWithRSJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.RSAccessionRecoveryJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.ResolveMergeThenSplitCandidatesJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.StudyClusteringJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc.DuplicateRSAccQCJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc.NewClusteredVariantsQCJobConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners.JobExecutionSetterConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.listeners.RSAccessionRecoveryJobListenerConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.processors.ClusteringVariantProcessorConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.recovery.RSAccessionRecoveryServiceConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.AccessioningShutdownStepConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.ClusteringFromMongoStepConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.ClusteringFromVcfStepConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.RSAccessionRecoveryStepConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.steps.qc.DuplicateRSAccQCStepConfiguration;
import uk.ac.ebi.eva.accession.clustering.runner.ClusteringCommandLineRunner;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import javax.sql.DataSource;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_MONGO_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERING_FROM_VCF_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_ACCESSION_RECOVERY_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.STUDY_CLUSTERING_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs.qc.NewClusteredVariantsQCJobConfiguration.NEW_CLUSTERED_VARIANTS_QC_JOB;

@EnableAutoConfiguration
@Import({ClusteringFromVcfJobConfiguration.class,
        ResolveMergeThenSplitCandidatesJobConfiguration.class,
        ClusteringFromMongoJobConfiguration.class,
        StudyClusteringJobConfiguration.class,
        NewClusteredVariantsQCJobConfiguration.class,
        ProcessRemappedVariantsWithRSJobConfiguration.class,
        ClusterUnclusteredVariantsJobConfiguration.class,
        BackPropagateRSJobConfiguration.class,
        ClusteringFromVcfStepConfiguration.class,
        ClusteringFromMongoStepConfiguration.class,
        VcfReaderConfiguration.class,
        RSMergeAndSplitCandidatesReaderConfiguration.class,
        RSMergeAndSplitWriterConfiguration.class,
        ClusteringMongoReaderConfiguration.class,
        ClusteringVariantProcessorConfiguration.class,
        ClusteringWriterConfiguration.class,
        TargetSSReaderForBackPropRSConfiguration.class,
        BackPropagatedRSWriterConfiguration.class,
        ListenersConfiguration.class,
        ClusteringCommandLineRunner.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        AccessioningShutdownStepConfiguration.class,
        JobExecutionSetterConfiguration.class,
        RSAccessionRecoveryJobConfiguration.class,
        RSAccessionRecoveryStepConfiguration.class,
        RSAccessionRecoveryServiceConfiguration.class,
        RSAccessionRecoveryJobListenerConfiguration.class,
        DuplicateRSAccQCJobConfiguration.class,
        DuplicateRSAccQCStepConfiguration.class,
        DuplicateRSAccQCFileReaderConfiguration.class,
        DuplicateRSAccQCProcessorConfiguration.class,
        DuplicateRSAccQCWriterConfiguration.class
})
public class BatchTestConfiguration {

    public static final String JOB_LAUNCHER_FROM_VCF = "JOB_LAUNCHER_FROM_VCF";

    public static final String JOB_LAUNCHER_FROM_MONGO = "JOB_LAUNCHER_FROM_MONGO";

    public static final String JOB_LAUNCHER_STUDY_FROM_MONGO = "JOB_LAUNCHER_STUDY_FROM_MONGO";

    public static final String JOB_LAUNCHER_NEW_CLUSTERED_VARIANTS_QC = "JOB_LAUNCHER_NEW_CLUSTERED_VARIANTS_QC";

    public static final String JOB_LAUNCHER_FROM_MONGO_ONLY_FIRST_STEP = "JOB_LAUNCHER_FROM_MONGO_ONLY_FIRST_STEP";

    public static final String JOB_LAUNCHER_RS_ACCESSION_RECOVERY = "JOB_LAUNCHER_RS_ACCESSION_RECOVERY";

    public static final String JOB_LAUNCHER_DUPLICATE_RS_ACC_QC_JOB = "JOB_LAUNCHER_DUPLICATE_RS_ACC_QC_JOB";

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

    @Bean(JOB_LAUNCHER_STUDY_FROM_MONGO)
    public JobLauncherTestUtils jobLauncherTestUtilsStudyFromMongo() {

        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(STUDY_CLUSTERING_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(JOB_LAUNCHER_RS_ACCESSION_RECOVERY)
    public JobLauncherTestUtils jobLauncherTestUtilsRSAccessionRecoveryJob() {

        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(RS_ACCESSION_RECOVERY_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(JOB_LAUNCHER_NEW_CLUSTERED_VARIANTS_QC)
    public JobLauncherTestUtils jobLauncherTestUtilsNewClusteredVariants() {

        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(NEW_CLUSTERED_VARIANTS_QC_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(JOB_LAUNCHER_FROM_MONGO_ONLY_FIRST_STEP)
    public JobLauncherTestUtils jobLauncherTestUtilsFromMongoOnlyFirstStep() {

        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(PROCESS_REMAPPED_VARIANTS_WITH_RS_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }

    @Bean(JOB_LAUNCHER_DUPLICATE_RS_ACC_QC_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsDuplicateRSAccQCJob() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(DUPLICATE_RS_ACC_QC_JOB) Job job) {
                super.setJob(job);
            }
        };
    }
}
