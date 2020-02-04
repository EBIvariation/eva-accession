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
package uk.ac.ebi.eva.accession.deprecate.configuration.batch.io;

import com.mongodb.MongoClient;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.deprecate.batch.io.DeprecableClusteredVariantsReader;
import uk.ac.ebi.eva.accession.deprecate.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECABLE_CLUSTERED_VARIANTS_READER;

@Configuration
@Import({MongoConfiguration.class})
public class DeprecableClusteredVariantsReaderConfiguration {

    @Bean(DEPRECABLE_CLUSTERED_VARIANTS_READER)
    @StepScope
    DeprecableClusteredVariantsReader deprecableClusteredVariantsReader(MongoClient mongoClient,
                                                                        MongoProperties mongoProperties,
                                                                        MongoTemplate mongoTemplate,
                                                                        InputParameters parameters) {
        boolean assembliesProvided =
                parameters.getAssemblyAccession() != null && !parameters.getAssemblyAccession().isEmpty();

        if (parameters.getDeprecateAll() == assembliesProvided) {
            throw new IllegalArgumentException(
                    "Please provide either: 1) parameters.deprecateAll=true and empty parameters.assemblyAccession or"
                    + " 2) parameters.deprecateAll=false and parameters.assemblyAccession=<comma-separated-accessions>");
        }
        if (assembliesProvided) {
            return new DeprecableClusteredVariantsReader(mongoClient, mongoProperties.getDatabase(), mongoTemplate,
                                                         parameters.getAssemblyAccession(), parameters.getChunkSize());
        } else {
            return new DeprecableClusteredVariantsReader(mongoClient, mongoProperties.getDatabase(), mongoTemplate,
                                                         parameters.getChunkSize());
        }
    }
}
