package uk.ac.ebi.eva.accession.core.configuration;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientOptions;
import com.mongodb.ReadPreference;
import com.mongodb.ServerAddress;
import com.mongodb.WriteConcern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.core.env.Environment;
import org.springframework.data.mongodb.MongoDbFactory;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.SimpleMongoDbFactory;
import org.springframework.data.mongodb.core.convert.DefaultMongoTypeMapper;
import org.springframework.data.mongodb.core.convert.MappingMongoConverter;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

import java.net.UnknownHostException;

@Configuration
@EnableMongoRepositories(basePackages = "uk.ac.ebi.eva.accession.core.repositoryHuman", mongoTemplateRef = "humanMongoTemplate")
@EntityScan(basePackages = {"uk.ac.ebi.eva.accession.core.repositoryHuman"})
//@Import({MongoDataAutoConfiguration.class})
public class MongoHumanConfiguration {

    @Value("${mongodb.read-preference}")
    private String readPreference;

    @Bean(name = "humanMongoProperties")
    @ConfigurationProperties(prefix = "human.mongodb")
    public MongoProperties mongoProperties() {
        return new MongoProperties();
    }

    @Bean("humanMongoClient")
    public MongoClient mongoClient(@Qualifier("humanMongoProperties") MongoProperties properties, ObjectProvider<MongoClientOptions> options,
                                        Environment environment) throws UnknownHostException {
        MongoClientOptions mongoClientOptions = options.getIfAvailable();
        MongoClientOptions.Builder mongoClientOptionsBuilder;
        if (mongoClientOptions != null) {
            mongoClientOptionsBuilder = new MongoClientOptions.Builder(mongoClientOptions);
        } else {
            mongoClientOptionsBuilder = new MongoClientOptions.Builder();
        }
        mongoClientOptions = mongoClientOptionsBuilder.readPreference(ReadPreference.valueOf(readPreference))
                                                      .writeConcern(WriteConcern.MAJORITY)
                                                      .build();
        return properties.createMongoClient(mongoClientOptions, environment);
        //return new MongoClient(new ServerAddress("localhost", 27017));

    }

    @Bean("humanFactory")
    public MongoDbFactory mongoDbFactory(@Qualifier("humanMongoProperties") MongoProperties properties,
                                         ObjectProvider<MongoClientOptions> options,
                                         Environment environment) throws UnknownHostException {
        return new SimpleMongoDbFactory(mongoClient(properties, options, environment), mongoProperties().getDatabase());
    }

    @Bean(name = "humanMongoTemplate")
    public MongoTemplate humanMongoTemplate(@Qualifier("humanFactory") MongoDbFactory mongoDbFactory,
                                            MappingMongoConverter converter) throws UnknownHostException {
        converter.setTypeMapper(new DefaultMongoTypeMapper(null));
        return new MongoTemplate(mongoDbFactory, converter);
        //return new MongoTemplate(new MongoClient(new ServerAddress("localhost", 27017)), "test");
    }
}
