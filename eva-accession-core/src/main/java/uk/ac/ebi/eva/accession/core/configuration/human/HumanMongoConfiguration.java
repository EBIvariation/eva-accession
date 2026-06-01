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
package uk.ac.ebi.eva.accession.core.configuration.human;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.MongoDatabaseFactory;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoClientDatabaseFactory;
import org.springframework.data.mongodb.core.WriteResultChecking;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.index.Index;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;
import uk.ac.ebi.eva.accession.core.configuration.MongoClientCreator;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;

import java.io.UnsupportedEncodingException;
import java.net.UnknownHostException;

@Configuration
@EnableMongoRepositories(basePackages = "uk.ac.ebi.eva.accession.core.repository.human.dbsnp", mongoTemplateRef = "humanMongoTemplate")
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.repositoryHuman"})
@EnableMongoAuditing
public class HumanMongoConfiguration {

    private static final String INACTIVE_OBJECTS_HASHED_MESSAGE = "inactiveObjects.hashedMessage";

    @Value("${mongodb.read-preference}")
    private String readPreference;

    @Bean(name = "humanMongoProperties")
    @ConfigurationProperties(prefix = "human.mongodb")
    public MongoProperties mongoProperties() {
        return new MongoProperties();
    }

    @Bean("humanMongoClient")
    public MongoClient mongoClient(@Qualifier("humanMongoProperties") MongoProperties properties,
                                   ObjectProvider<MongoClientSettings> settings)
            throws UnknownHostException, UnsupportedEncodingException {
        return MongoClientCreator.getMongoClient(properties, settings, readPreference);
    }

    @Bean("humanFactory")
    public MongoDatabaseFactory mongoDbFactory(@Qualifier("humanMongoProperties") MongoProperties properties,
                                               ObjectProvider<MongoClientSettings> settings)
            throws UnknownHostException, UnsupportedEncodingException {
        return new SimpleMongoClientDatabaseFactory(mongoClient(properties, settings), properties.getDatabase());
    }

    @Bean("humanMappingConverter")
    public MappingMongoConverter mappingMongoConverter(MongoProperties properties,
                                                       ObjectProvider<MongoClientSettings> settings)
            throws UnknownHostException, UnsupportedEncodingException {
        return new MappingMongoConverter(new DefaultDbRefResolver(mongoDbFactory(properties, settings)),
                new MongoMappingContext());
    }

    @Bean(name = "humanMongoTemplate")
    public MongoTemplate humanMongoTemplate(@Qualifier("humanFactory") MongoDatabaseFactory mongoDbFactory,
                                            @Qualifier("humanMappingConverter") MappingMongoConverter converter) {
        converter.setTypeMapper(new DefaultMongoTypeMapper(null));
        MongoTemplate mongoTemplate = new MongoTemplate(mongoDbFactory, converter);
        mongoTemplate.setWriteResultChecking(WriteResultChecking.EXCEPTION);
        mongoTemplate.indexOps(DbsnpClusteredVariantOperationEntity.class).ensureIndex(
                new Index().on(INACTIVE_OBJECTS_HASHED_MESSAGE, Sort.Direction.ASC).background());
        return mongoTemplate;
    }
}
