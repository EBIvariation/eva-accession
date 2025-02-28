/*
 *
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
package uk.ac.ebi.eva.accession.core.test.configuration.human;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationPropertiesConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.human.HumanClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.human.HumanMongoConfiguration;

@Configuration
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.repository.human"})
@EnableMongoRepositories(basePackages = "uk.ac.ebi.eva.accession.core.repository.human", mongoTemplateRef = "humanMongoTemplate")
@Import({HumanMongoConfiguration.class,
        HumanClusteredVariantAccessioningConfiguration.class,
        ApplicationPropertiesConfiguration.class
        })
@EnableConfigurationProperties(HumanMongoConfiguration.class)
public class MongoHumanTestConfiguration {
}
