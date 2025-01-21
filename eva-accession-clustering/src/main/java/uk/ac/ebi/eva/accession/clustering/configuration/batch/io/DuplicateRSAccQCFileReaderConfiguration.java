package uk.ac.ebi.eva.accession.clustering.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.DuplicateRSAccQCFileReader;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_FILE_READER;

@Configuration
public class DuplicateRSAccQCFileReaderConfiguration {

    @Bean(DUPLICATE_RS_ACC_QC_FILE_READER)
    @StepScope
    DuplicateRSAccQCFileReader duplicateRSAccQCFileReader(InputParameters parameters) {
        return new DuplicateRSAccQCFileReader(parameters.getRsAccFile(), parameters.getChunkSize());
    }
}
