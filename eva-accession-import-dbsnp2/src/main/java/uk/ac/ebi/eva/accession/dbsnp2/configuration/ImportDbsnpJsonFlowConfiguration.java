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
package uk.ac.ebi.eva.accession.dbsnp2.configuration;

import org.springframework.batch.core.Step;
import org.springframework.batch.core.job.builder.FlowBuilder;
import org.springframework.batch.core.job.flow.Flow;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_FLOW;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_STEP;

/**
 * Configuration class that describes flow process in the import dbSNP JSON job.
 * <p>
 * This flow executes imports the dbSNP JSON and persists it into the data store.
 */
@Configuration
public class ImportDbsnpJsonFlowConfiguration {

    @Autowired
    @Qualifier(IMPORT_DBSNP_JSON_VARIANTS_STEP)
    private Step importDbsnpJsonVariantsStep;

    @Bean(IMPORT_DBSNP_JSON_VARIANTS_FLOW)
    public Flow importDbsnpJsonFlow() {
        return new FlowBuilder<Flow>(IMPORT_DBSNP_JSON_VARIANTS_FLOW)
                .start(importDbsnpJsonVariantsStep)
                .build();
    }
}
