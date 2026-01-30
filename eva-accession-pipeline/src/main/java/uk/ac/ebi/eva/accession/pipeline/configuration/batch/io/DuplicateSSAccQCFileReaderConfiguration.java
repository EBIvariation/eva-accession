package uk.ac.ebi.eva.accession.pipeline.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.pipeline.batch.io.DuplicateSSAccQCFileReader;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.DUPLICATE_SS_ACC_QC_FILE_READER;

@Configuration
public class DuplicateSSAccQCFileReaderConfiguration {

    @Bean(DUPLICATE_SS_ACC_QC_FILE_READER)
    @StepScope
    DuplicateSSAccQCFileReader duplicateSSAccQCFileReader(InputParameters parameters) {
        return new DuplicateSSAccQCFileReader(parameters.getOutputVcf(), parameters.getChunkSize());
    }
}
