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

import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import uk.ac.ebi.eva.accession.core.configuration.ApplicationPropertiesConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.human.HumanClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.human.HumanMongoConfiguration;

import java.util.Collections;

@Configuration
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.repository.human"})
@EnableMongoRepositories(basePackages = "uk.ac.ebi.eva.accession.core.repository.human", mongoTemplateRef = "humanMongoTemplate")
@Import({HumanMongoConfiguration.class,
        HumanClusteredVariantAccessioningConfiguration.class,
        ApplicationPropertiesConfiguration.class
})
@EnableConfigurationProperties(HumanMongoConfiguration.class)
public class MongoHumanTestConfiguration {

    @Value("${spring.data.mongodb.uri}")
    private String mongoUri;

    @Value("${spring.data.mongodb.database}")
    private String database;

    @Value("${mongodb.read-preference:primary}")
    private String readPreference;

    @Bean
    @Primary
    public MongoCustomConversions mongoCustomConversions() {
        return new MongoCustomConversions(Collections.emptyList());
    }

    @Bean
    @Primary
    public MongoMappingContext mongoMappingContext(MongoCustomConversions conversions) {
        MongoMappingContext context = new MongoMappingContext();
        context.setSimpleTypeHolder(conversions.getSimpleTypeHolder());
        context.setAutoIndexCreation(false);
        return context;
    }


    @Bean("humanMongoClient")
    public MongoClient humanMongoClient() {
        return MongoClients.create(mongoUri);
    }

    @Bean("humanFactory")
    public MongoDatabaseFactory humanMongoDbFactory() {
        return new SimpleMongoClientDatabaseFactory(humanMongoClient(), database);
    }

    @Bean("humanMongoTemplate")
    public MongoTemplate humanMongoTemplate() {
        MongoMappingContext context = new MongoMappingContext();
        MappingMongoConverter converter = new MappingMongoConverter(
                new DefaultDbRefResolver(humanMongoDbFactory()), context);
        converter.setTypeMapper(new DefaultMongoTypeMapper(null));
        converter.afterPropertiesSet();
        MongoTemplate template = new MongoTemplate(humanMongoDbFactory(), converter);
        template.setReadPreference(ReadPreference.valueOf(readPreference));
        template.setWriteConcern(WriteConcern.MAJORITY);
        template.setWriteResultChecking(WriteResultChecking.EXCEPTION);
        return template;
    }
}
