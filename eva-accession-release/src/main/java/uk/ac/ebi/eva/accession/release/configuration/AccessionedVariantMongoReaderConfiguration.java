package uk.ac.ebi.eva.accession.release.configuration;

import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACCESSIONED_VARIANT_READER;

@Configuration
@Import({MongoConfiguration.class})
public class AccessionedVariantMongoReaderConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(AccessionedVariantMongoReader.class);

    @Bean(name = ACCESSIONED_VARIANT_READER)
    @StepScope
    AccessionedVariantMongoReader accessionedVariantMongoReader(InputParameters parameters, MongoClient mongoClient,
                                                                MongoProperties mongoProperties) {
        logger.info("Injecting AccessionedVariantMongoReader with parameters: {}", parameters);
        return new AccessionedVariantMongoReader(parameters.getAssemblyAccession(), mongoClient,
                                                 mongoProperties.getDatabase());
    }
}
