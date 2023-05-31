package uk.ac.ebi.eva.accession.core.configuration;

import com.mongodb.*;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.mongo.MongoClientFactory;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.core.env.Environment;
import uk.ac.ebi.eva.commons.mongodb.utils.MongoUtils;

import java.io.UnsupportedEncodingException;
import java.util.Objects;

public class MongoClientCreator {
    public static MongoClient getMongoClient(MongoProperties properties, ObjectProvider<MongoClientOptions> options,
                                             Environment environment, String readPreference)
            throws UnsupportedEncodingException {
        MongoClientOptions mongoClientOptions = options.getIfAvailable();
        // Only set the URI if it isn't already set
        if (Objects.isNull(properties.getUri())) {
            // Weirdly, MongoClient instantiation works without authentication mechanism
            // in the eva-accession project but does not work in the eva-pipeline project
            // So we explicitly pass it as null here
            properties.setUri(MongoUtils.constructMongoClientURI(properties.getHost(), properties.getPort(),
                    properties.getDatabase(), properties.getUsername(), (Objects.nonNull(properties.getPassword()) ?
                            String.valueOf(properties.getPassword()) : ""),
                    properties.getAuthenticationDatabase(), null, readPreference).getURI());
        }
        properties.setHost(null);
        MongoClientOptions.Builder mongoClientOptionsBuilder;
        if (mongoClientOptions != null) {
            mongoClientOptionsBuilder = new MongoClientOptions.Builder(mongoClientOptions);
        } else {
            mongoClientOptionsBuilder = new MongoClientOptions.Builder();
        }
        mongoClientOptions = mongoClientOptionsBuilder.readPreference(ReadPreference.valueOf(readPreference))
                .writeConcern(WriteConcern.MAJORITY).readConcern(ReadConcern.MAJORITY).build();
        return new MongoClientFactory(properties, environment).createMongoClient(mongoClientOptions);
    }
}
