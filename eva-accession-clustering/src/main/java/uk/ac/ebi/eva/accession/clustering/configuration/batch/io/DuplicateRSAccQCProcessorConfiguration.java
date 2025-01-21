package uk.ac.ebi.eva.accession.clustering.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.DuplicateRSAccQCProcessor;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_PROCESSOR;

@Configuration
@Import({MongoConfiguration.class})
public class DuplicateRSAccQCProcessorConfiguration {

    @Bean(DUPLICATE_RS_ACC_QC_PROCESSOR)
    @StepScope
    DuplicateRSAccQCProcessor duplicateRSAccQCProcessor(MongoTemplate mongoTemplate) {
        return new DuplicateRSAccQCProcessor(mongoTemplate);
    }
}
