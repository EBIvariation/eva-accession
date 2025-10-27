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

package uk.ac.ebi.eva.accession.pipeline.test;

import org.springframework.batch.core.Job;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.io.AccessionWriterConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.io.VcfReaderConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs.CreateSubsnpAccessionsJobConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs.QCSubsnpAccessionsJobConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.jobs.SubsnpAccessionsJobConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners.SubsnpAccessionJobExecutionListener;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.policies.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.policies.InvalidVariantSkipPolicyConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.processors.VariantProcessorConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps.AccessioningShutdownStepConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps.BuildReportStepConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps.CreateSubsnpAccessionsStepConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps.QCSubsnpAccessionsStepConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps.SubsnpAccessionsStepConfiguration;
import uk.ac.ebi.eva.accession.pipeline.runner.EvaAccessionJobLauncherCommandLineRunner;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.QC_SUBSNP_ACCESSION_JOB;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SUBSNP_ACCESSION_JOB;

@EnableAutoConfiguration
@Import({CreateSubsnpAccessionsJobConfiguration.class, CreateSubsnpAccessionsStepConfiguration.class,
        SubsnpAccessionsJobConfiguration.class, SubsnpAccessionsStepConfiguration.class,
        QCSubsnpAccessionsJobConfiguration.class, QCSubsnpAccessionsStepConfiguration.class,
        VcfReaderConfiguration.class, VariantProcessorConfiguration.class, AccessionWriterConfiguration.class,
        BuildReportStepConfiguration.class, AccessioningShutdownStepConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class, InvalidVariantSkipPolicyConfiguration.class,
        EvaAccessionJobLauncherCommandLineRunner.class, SubsnpAccessionJobExecutionListener.class})
public class BatchTestConfiguration {
    public static final String JOB_LAUNCHER_SUBSNP_ACCESSION_JOB = "JOB_LAUNCHER_SUBSNP_ACCESSION_JOB";
    public static final String JOB_LAUNCHER_QC_SUBSNP_ACCESSION_JOB = "JOB_LAUNCHER_QC_SUBSNP_ACCESSION_JOB";

    @Bean(JOB_LAUNCHER_SUBSNP_ACCESSION_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsCreate() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(SUBSNP_ACCESSION_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(JOB_LAUNCHER_QC_SUBSNP_ACCESSION_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsQC() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(QC_SUBSNP_ACCESSION_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }
}