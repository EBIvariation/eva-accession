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

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.config.EnableMongoAuditing;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.net.UnknownHostException;

@Configuration
@EnableMongoRepositories(basePackages = {"uk.ac.ebi.eva.accession.core.persistence"})
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.persistence"})
@EnableMongoAuditing
@Import({MongoDataAutoConfiguration.class})
public class MongoConfiguration {

    @Value("${mongodb.read-preference}")
    private ReadPreference readPreference;

    @Bean
    public MongoClient mongoClient(MongoProperties properties, ObjectProvider<MongoClientOptions> options,
                            Environment environment) throws UnknownHostException {
        MongoClientOptions mongoClientOptions = options.getIfAvailable();
        if (mongoClientOptions != null) {
            mongoClientOptions = new MongoClientOptions.Builder(mongoClientOptions).readPreference(readPreference)
                                                                                   .build();
        } else {
            mongoClientOptions = new MongoClientOptions.Builder().readPreference(readPreference).build();
        }
        return properties.createMongoClient(mongoClientOptions, environment);
    }

}
