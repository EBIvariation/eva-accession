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
 */

package uk.ac.ebi.eva.accession.remapping.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.remapping.configuration.batch.steps.ExportSubmittedVariantsStepConfiguration;

import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EXPORT_EVA_SUBMITTED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.remapping.configuration.BeanNames.EXPORT_SUBMITTED_VARIANTS_JOB;

@Configuration
@EnableBatchProcessing
@Import({ExportSubmittedVariantsStepConfiguration.class})
public class ExportSubmittedVariantsJobConfiguration {

    @Bean(EXPORT_SUBMITTED_VARIANTS_JOB)
    public Job accessionReleaseJob(
            JobBuilderFactory jobBuilderFactory,
            @Autowired @Qualifier(EXPORT_EVA_SUBMITTED_VARIANTS_STEP) Step exportEvaSubmittedVariantsStep
//            ,
//            @Autowired @Qualifier(EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP) Step exportDbsnpSubmittedVariantsStep
    ) {
        return jobBuilderFactory.get(EXPORT_SUBMITTED_VARIANTS_JOB)
                                .incrementer(new RunIdIncrementer())
                                .start(exportEvaSubmittedVariantsStep)
//                                .next(exportDbsnpSubmittedVariantsStep)   // TODO
                                .build();
    }
}
