package uk.ac.ebi.eva.accession.pipeline.configuration.batch.processors;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.pipeline.batch.processors.DuplicateSSAccQCProcessor;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.DUPLICATE_SS_ACC_QC_PROCESSOR;

@Configuration
@Import({MongoConfiguration.class})
public class DuplicateSSAccQCProcessorConfiguration {

    @Bean(DUPLICATE_SS_ACC_QC_PROCESSOR)
    @StepScope
    DuplicateSSAccQCProcessor duplicateSSAccQCProcessor(MongoTemplate mongoTemplate) {
        return new DuplicateSSAccQCProcessor(mongoTemplate);
    }
}
