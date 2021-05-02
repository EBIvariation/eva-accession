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

package uk.ac.ebi.eva.remapping.source.configuration.batch.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.remapping.source.batch.io.DbsnpSubmittedVariantMongoReader;
import uk.ac.ebi.eva.remapping.source.batch.io.EvaSubmittedVariantMongoReader;
import uk.ac.ebi.eva.remapping.source.parameters.InputParameters;
import uk.ac.ebi.eva.remapping.source.configuration.BeanNames;

@Configuration
@Import({MongoConfiguration.class})
public class SubmittedVariantMongoReaderConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(SubmittedVariantMongoReaderConfiguration.class);

    @Bean(BeanNames.EVA_SUBMITTED_VARIANT_READER)
    @StepScope
    EvaSubmittedVariantMongoReader evaSubmittedVariantMongoReader(InputParameters parameters,
                                                                  MongoTemplate mongoTemplate) {
        logger.info("Injecting EvaSubmittedVariantMongoReader with parameters: {}", parameters);
        if (parameters.getProjects() == null || parameters.getProjects().isEmpty()) {
            return new EvaSubmittedVariantMongoReader(parameters.getAssemblyAccession(), mongoTemplate);
        } else {
            return new EvaSubmittedVariantMongoReader(parameters.getAssemblyAccession(), mongoTemplate,
                                                      parameters.getProjects());
        }
    }

    @Bean(BeanNames.DBSNP_SUBMITTED_VARIANT_READER)
    @StepScope
    DbsnpSubmittedVariantMongoReader dbsnpSubmittedVariantMongoReader(InputParameters parameters,
                                                                      MongoTemplate mongoTemplate) {
        logger.info("Injecting DbsnpSubmittedVariantMongoReader with parameters: {}", parameters);
        if (parameters.getProjects() == null || parameters.getProjects().isEmpty()) {
            return new DbsnpSubmittedVariantMongoReader(parameters.getAssemblyAccession(), mongoTemplate);
        } else {
            return new DbsnpSubmittedVariantMongoReader(parameters.getAssemblyAccession(), mongoTemplate,
                                                        parameters.getProjects());
        }
    }
}
