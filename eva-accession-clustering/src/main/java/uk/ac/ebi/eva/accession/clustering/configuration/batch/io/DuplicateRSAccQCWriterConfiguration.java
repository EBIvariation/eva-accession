package uk.ac.ebi.eva.accession.clustering.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.clustering.batch.io.qc.DuplicateRSAccQCWriter;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.DUPLICATE_RS_ACC_QC_WRITER;


@Configuration
public class DuplicateRSAccQCWriterConfiguration {
    @Bean(DUPLICATE_RS_ACC_QC_WRITER)
    @StepScope
    DuplicateRSAccQCWriter duplicateRSAccQCWrite(InputParameters parameters) {
        return new DuplicateRSAccQCWriter(parameters.getDuplicateRSAccFile());
    }
}
