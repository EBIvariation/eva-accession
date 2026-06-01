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
package uk.ac.ebi.eva.accession.core.configuration.nonhuman;

import com.mongodb.MongoClientSettings;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.convert.MongoCustomConversions;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import uk.ac.ebi.eva.accession.core.configuration.MongoClientCreator;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;
import java.util.Collections;

@Configuration
@EnableMongoRepositories(basePackages = {"uk.ac.ebi.eva.accession.core.repository"})
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.persistence"})
@EnableMongoAuditing
public class MongoConfiguration {

    @Value("${mongodb.read-preference}")
    private String readPreference;

    @Value("${parameters.mongodb.writeConcern:#{null}}")
    private String writeConcern;

    @Primary
    @Bean
    @ConfigurationProperties(prefix = "spring.data.mongodb")
    public MongoProperties mongoProperties() {
        return new MongoProperties();
    }

    @Bean
    @Primary
    public MongoClient mongoClient(MongoProperties properties, ObjectProvider<MongoClientSettings> settings)
            throws UnknownHostException, UnsupportedEncodingException {
        return MongoClientCreator.getMongoClient(properties, settings, readPreference);
    }

    @Bean("primaryFactory")
    @Primary
    public MongoDatabaseFactory mongoDbFactory(MongoProperties properties, ObjectProvider<MongoClientSettings> settings)
            throws UnknownHostException, UnsupportedEncodingException {
        return new SimpleMongoClientDatabaseFactory(mongoClient(properties, settings), properties.getDatabase());
    }

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

    @Bean
    @Primary
    public MappingMongoConverter mappingMongoConverter(MongoProperties properties,
                                                       ObjectProvider<MongoClientSettings> settings,
                                                       MongoMappingContext mongoMappingContext,
                                                       MongoCustomConversions conversions)
            throws UnknownHostException, UnsupportedEncodingException {
        MappingMongoConverter converter = new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory(properties, settings)),
                mongoMappingContext);
        converter.setCustomConversions(conversions);
        converter.afterPropertiesSet();
        return converter;
    }

    @Primary
    @Bean
    public MongoTemplate mongoTemplate(@Qualifier("primaryFactory") MongoDatabaseFactory mongoDbFactory,
                                       MappingMongoConverter converter) {
        converter.setTypeMapper(new DefaultMongoTypeMapper(null));
        MongoTemplate mongoTemplate = new MongoTemplate(mongoDbFactory, converter);
        mongoTemplate.setReadPreference(ReadPreference.valueOf(readPreference));

        if (writeConcern != null && !writeConcern.isEmpty() && WriteConcern.valueOf(writeConcern) != null) {
            mongoTemplate.setWriteConcern(WriteConcern.valueOf(writeConcern));
        } else {
            mongoTemplate.setWriteConcern(WriteConcern.MAJORITY);
        }

        mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);
        return mongoTemplate;
    }
}
