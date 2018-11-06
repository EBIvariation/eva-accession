package uk.ac.ebi.eva.accession.release.configuration;

import com.mongodb.MongoClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.boot.autoconfigure.mongo.MongoProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.release.io.AccessionedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.ACCESSIONED_VARIANT_READER;

@Configuration
@Import({MongoConfiguration.class})
public class AccessionedVariantMongoReaderConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(AccessionedVariantMongoReader.class);

    @Bean(ACCESSIONED_VARIANT_READER)
    @StepScope
    public ItemStreamReader<Variant> unwindingReader(AccessionedVariantMongoReader accessionedVariantMongoReader) {
        return new UnwindingItemStreamReader<>(accessionedVariantMongoReader);
    }

    @Bean
    @StepScope
    AccessionedVariantMongoReader accessionedVariantMongoReader(InputParameters parameters, MongoClient mongoClient,
                                                                MongoProperties mongoProperties) {
        logger.info("Injecting AccessionedVariantMongoReader with parameters: {}", parameters);
        return new AccessionedVariantMongoReader(parameters.getAssemblyAccession(), mongoClient,
                                                 mongoProperties.getDatabase());
    }
}
