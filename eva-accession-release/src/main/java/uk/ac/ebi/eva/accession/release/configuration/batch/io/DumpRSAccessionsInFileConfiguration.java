package uk.ac.ebi.eva.accession.release.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.eva.accession.release.batch.io.DumpRSAccessionsInFile;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_RS_ACCESSIONS_IN_FILE;

@Configuration
public class DumpRSAccessionsInFileConfiguration {

    @Bean(DUMP_RS_ACCESSIONS_IN_FILE)
    @StepScope
    DumpRSAccessionsInFile dumpRSAccessionsInFile(MongoTemplate mongoTemplate, InputParameters inputParameters) {
        return new DumpRSAccessionsInFile(mongoTemplate, inputParameters.getRsAccDumpFile(), inputParameters.getChunkSize());
    }
}
