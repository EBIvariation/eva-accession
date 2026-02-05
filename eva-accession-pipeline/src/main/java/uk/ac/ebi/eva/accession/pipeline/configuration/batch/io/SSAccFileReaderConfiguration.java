package uk.ac.ebi.eva.accession.pipeline.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.pipeline.batch.io.SSAccFileReader;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SS_ACC_FILE_READER;


@Configuration
public class SSAccFileReaderConfiguration {

    @Bean(SS_ACC_FILE_READER)
    @StepScope
    SSAccFileReader ssAccFileReader(InputParameters parameters) {
        return new SSAccFileReader(parameters.getOutputVcf(), parameters.getChunkSize());
    }
}
