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
 *
 */
package uk.ac.ebi.eva.accession.pipeline.configuration.batch.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.transaction.PlatformTransactionManager;
import uk.ac.ebi.eva.accession.pipeline.batch.tasklets.buildReport.BuildReportTasklet;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import java.io.File;

import static uk.ac.ebi.eva.accession.core.configuration.InMemoryBatchConfiguration.BATCH_TRANSACTION_MANAGER;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.BUILD_REPORT_STEP;

@Configuration
public class BuildReportStepConfiguration {

    @Autowired
    private InputParameters inputParameters;

    @Bean(BUILD_REPORT_STEP)
    public Step buildReportStep(JobRepository jobRepository,
                                @Qualifier(BATCH_TRANSACTION_MANAGER) PlatformTransactionManager transactionManager) {
        BuildReportTasklet tasklet = new BuildReportTasklet(new File(inputParameters.getOutputVcf()));
        TaskletStep step = new StepBuilder(BUILD_REPORT_STEP, jobRepository)
                .tasklet(tasklet, transactionManager)
                .build();
        return step;
    }
}
