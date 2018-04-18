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
package uk.ac.ebi.eva.accession.core.configuration;

import com.mongodb.Mongo;
import com.mongodb.MongoClient;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.domain.AuditorAware;
import org.springframework.data.mongodb.config.AbstractMongoConfiguration;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.util.Collection;
import java.util.Collections;

@Configuration
@EnableMongoRepositories(basePackages = {"uk.ac.ebi.eva.accession.core.persistence"})
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.persistence"})
@EnableMongoAuditing
@Import(MongoAutoConfiguration.class)
public class MongoConfiguration extends AbstractMongoConfiguration {

    private MongoClient mongoClient;

    private MongoProperties mongoProperties;

    public MongoConfiguration(MongoClient mongoClient, MongoProperties mongoProperties) {
        this.mongoClient = mongoClient;
        this.mongoProperties = mongoProperties;
    }

    @Bean
    public MongoTemplate mongoTemplate() {
        return new MongoTemplate(mongo(), getDatabaseName());
    }

    @Override
    protected String getDatabaseName() {
        return mongoProperties.getDatabase();
    }

    @Override
    public Mongo mongo() {
        return mongoClient;
    }

    @Override
    protected Collection<String> getMappingBasePackages() {
        return Collections.singletonList("uk.ac.ebi.eva.accession.core.persistence");
    }

}
