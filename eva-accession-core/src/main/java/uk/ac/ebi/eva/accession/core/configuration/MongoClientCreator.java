package uk.ac.ebi.eva.accession.core.configuration;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.ReadConcern;
import com.mongodb.ReadPreference;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import uk.ac.ebi.eva.commons.mongodb.utils.MongoUtils;

import java.util.Objects;

public class MongoClientCreator {
    public static MongoClient getMongoClient(MongoProperties properties, ObjectProvider<MongoClientSettings> settings,
                                             String readPreference) {
        MongoClientSettings mongoClientSettings = settings.getIfAvailable();
        // Only set the URI if it isn't already set
        if (Objects.isNull(properties.getUri())) {
            // Weirdly, MongoClient instantiation works without authentication mechanism
            // in the eva-accession project but does not work in the eva-pipeline project
            // So we explicitly pass it as null here
            properties.setUri(MongoUtils.constructMongoConnectionString(properties.getHost(), properties.getPort(),
                    properties.getDatabase(), properties.getUsername(), (Objects.nonNull(properties.getPassword()) ?
                            String.valueOf(properties.getPassword()) : ""),
                    properties.getAuthenticationDatabase(), null, readPreference).getConnectionString());
        }
        // If we don't do this Spring gets confused since both the URI (which already has the host)
        // and the host parameters are present
        properties.setHost(null);
        MongoClientSettings.Builder mongoClientSettingsBuilder;
        if (mongoClientSettings != null) {
            mongoClientSettingsBuilder = MongoClientSettings.builder(mongoClientSettings);
        } else {
            mongoClientSettingsBuilder = MongoClientSettings.builder();
        }
        mongoClientSettings = mongoClientSettingsBuilder.readPreference(ReadPreference.valueOf(readPreference))
                .writeConcern(WriteConcern.MAJORITY).readConcern(ReadConcern.MAJORITY)
                .applyConnectionString(new ConnectionString(properties.getUri())).build();
        return MongoClients.create(mongoClientSettings);
    }
}
