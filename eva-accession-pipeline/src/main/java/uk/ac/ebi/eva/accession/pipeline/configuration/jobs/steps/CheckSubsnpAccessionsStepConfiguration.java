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
 *
 */
package uk.ac.ebi.eva.accession.pipeline.configuration.jobs.steps;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck.CoordinatesVcfLineMapper;
import uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck.ReportCheckTasklet;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;

@Configuration
@EnableBatchProcessing
public class CheckSubsnpAccessionsStepConfiguration {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private VcfReader vcfReader;

    @Bean
    public VcfReader reportVcfReader() throws IOException {
        return new VcfReader(new CoordinatesVcfLineMapper(), new File(inputParameters.getOutputVcf()));
    }

    @Bean(CHECK_SUBSNP_ACCESSION_STEP)
    public Step checkSubsnpAccessionStep(StepBuilderFactory stepBuilderFactory) throws IOException {
        TaskletStep step = stepBuilderFactory.get(CHECK_SUBSNP_ACCESSION_STEP)
                                             .tasklet(new ReportCheckTasklet(vcfReader, reportVcfReader()))
                                             .build();
        return step;
    }
}