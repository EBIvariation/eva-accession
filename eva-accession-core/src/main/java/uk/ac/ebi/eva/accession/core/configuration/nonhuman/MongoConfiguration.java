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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;

import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import uk.ac.ebi.eva.accession.core.configuration.MongoClientCreator;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;

@Configuration
@EnableMongoRepositories(basePackages = {"uk.ac.ebi.eva.accession.core.repository"})
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.persistence"})
@EnableMongoAuditing
public class MongoConfiguration {

    @Value("${mongodb.read-preference}")
    private String readPreference;

    @Primary
    @Bean
    @ConfigurationProperties(prefix = "spring.data.mongodb")
    public MongoProperties mongoProperties() {
        return new MongoProperties();
    }

    @Bean
    @Primary
    public MongoClient mongoClient(MongoProperties properties, ObjectProvider<MongoClientOptions> options,
                                   Environment environment) throws UnknownHostException, UnsupportedEncodingException {
        return MongoClientCreator.getMongoClient(properties, options, environment, readPreference);
    }

    @Bean("primaryFactory")
    @Primary
    public MongoDbFactory mongoDbFactory(MongoProperties properties,
                                         ObjectProvider<MongoClientOptions> options,
                                         Environment environment)
            throws UnknownHostException, UnsupportedEncodingException {
        return new SimpleMongoDbFactory(mongoClient(properties, options, environment), properties.getDatabase());
    }

    @Bean
    @Primary
    public MappingMongoConverter mappingMongoConverter(MongoProperties properties,
                                                       ObjectProvider<MongoClientOptions> options,
                                                       Environment environment)
            throws UnknownHostException, UnsupportedEncodingException {
        return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory(properties, options, environment)),
                                         new MongoMappingContext());
    }

    @Primary
    @Bean
    public MongoTemplate mongoTemplate(@Qualifier("primaryFactory") MongoDbFactory mongoDbFactory,
                                       MappingMongoConverter converter) {
        converter.setTypeMapper(new DefaultMongoTypeMapper(null));
        return new MongoTemplate(mongoDbFactory, converter);
    }
}
