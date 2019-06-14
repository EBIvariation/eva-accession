package uk.ac.ebi.eva.accession.dbsnp.configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.autoconfigure.ImportAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultDbRefResolver;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.core.mapping.MongoMappingContext;


import java.net.UnknownHostException;
import java.util.Collections;

/**
 * Utility class to setup MongoDB connection
 */
@Configuration
@ImportAutoConfiguration(MongoAutoConfiguration.class)
public class MongoConfiguration {

    @Bean
    public MongoMappingContext mongoMappingContext() {
        return new MongoMappingContext();
    }

    @Bean
    @StepScope
    public MongoOperations mongoOperations(MongoMappingContext mongoMappingContext)
        throws UnknownHostException {
        MongoDbFactory mongoDbFactory = new SimpleMongoDbFactory(getMongoClient(), "admin");

        DbRefResolver dbRefResolver = new DefaultDbRefResolver(mongoDbFactory);
        MappingMongoConverter mappingMongoConverter = new MappingMongoConverter(dbRefResolver, mongoMappingContext);
        mappingMongoConverter.setTypeMapper(new DefaultMongoTypeMapper(null));

        mappingMongoConverter.setMapKeyDotReplacement("EVA");
        return new MongoTemplate(mongoDbFactory, mappingMongoConverter);
    }

    private static MongoClient getMongoClient() throws UnknownHostException {
        String authenticationDatabase = "admin";
        String user = "appAdmin";
        String password = "evapipeline";
        MongoClient mongoClient;
        mongoClient = new MongoClient(
            new ServerAddress("127.0.0.1", 27017),
            Collections.singletonList(MongoCredential.createCredential(user,
                authenticationDatabase, password.toCharArray())));
        return mongoClient;
    }
}