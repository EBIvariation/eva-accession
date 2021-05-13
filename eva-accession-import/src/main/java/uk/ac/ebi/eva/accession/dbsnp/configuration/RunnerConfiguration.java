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
package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.springframework.batch.core.configuration.annotation.BatchConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.commons.batch.configuration.SpringBoot1CompatibilityConfiguration;
import uk.ac.ebi.eva.commons.batch.job.JobExecutionApplicationListener;

import javax.persistence.EntityManagerFactory;
import javax.sql.DataSource;

@Configuration
public class RunnerConfiguration {

    @Bean
    public JobExecutionApplicationListener jobExecutionApplicationListener() {
        return new JobExecutionApplicationListener();
    }

    @Bean
    public BatchConfigurer configurer(DataSource dataSource, EntityManagerFactory entityManagerFactory)
            throws Exception {
        return SpringBoot1CompatibilityConfiguration.getSpringBoot1CompatibleBatchConfigurer(dataSource,
                entityManagerFactory);
    }
}