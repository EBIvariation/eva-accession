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
package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.batch.io.deprecated.DeprecatedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DBSNP_DEPRECATED_VARIANT_READER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EVA_DEPRECATED_VARIANT_READER;

@Configuration
@Import({MongoConfiguration.class})
public class DeprecatedVariantMongoReaderConfiguration {

    @Bean(DBSNP_DEPRECATED_VARIANT_READER)
    @StepScope
    public DeprecatedVariantMongoReader deprecatedVariantMongoReaderDbsnp(InputParameters parameters,
                                                                          MongoTemplate mongoTemplate) {
        return DeprecatedVariantMongoReader.evaDeprecatedVariantMongoReader(parameters.getAssemblyAccession(),
                                                                            mongoTemplate);
    }

    @Bean(EVA_DEPRECATED_VARIANT_READER)
    @StepScope
    public DeprecatedVariantMongoReader deprecatedVariantMongoReaderEva(InputParameters parameters,
                                                                        MongoTemplate mongoTemplate) {
        return DeprecatedVariantMongoReader.dbsnpDeprecatedVariantMongoReader(parameters.getAssemblyAccession(),
                                                                              mongoTemplate);
    }

}
