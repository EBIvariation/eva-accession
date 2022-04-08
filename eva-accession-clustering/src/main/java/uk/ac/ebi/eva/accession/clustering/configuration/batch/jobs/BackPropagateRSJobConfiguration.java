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
package uk.ac.ebi.eva.accession.clustering.configuration.batch.jobs;

import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_RS_JOB;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_NEW_RS_STEP;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATE_SPLIT_MERGED_RS_STEP;

@Configuration
@EnableBatchProcessing
public class BackPropagateRSJobConfiguration {
    // Deal with back-propagation of RS, that were assigned to SS in the remapped assembly, to the original assembly
    // Can be parallelized across multiple species
    @Bean(BACK_PROPAGATE_RS_JOB)
    public Job processRemappedVariantsWithRSJob(
            // Back-propagate RS that were newly created in the remapped assembly
            @Qualifier(BACK_PROPAGATE_NEW_RS_STEP) Step backPropagateNewRSStep,
            // Back-propagate RS in the remapped assembly that were split or merged
            @Qualifier(BACK_PROPAGATE_SPLIT_MERGED_RS_STEP) Step backPropagateSplitMergedRSStep,
            JobBuilderFactory jobBuilderFactory) {
        return jobBuilderFactory.get(BACK_PROPAGATE_RS_JOB)
                .incrementer(new RunIdIncrementer())
                .start(backPropagateNewRSStep)
                .next(backPropagateSplitMergedRSStep)
                .build();
    }
}
