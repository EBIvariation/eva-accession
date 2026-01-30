package uk.ac.ebi.eva.accession.pipeline.configuration.batch.io;

import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import uk.ac.ebi.eva.accession.pipeline.batch.io.DuplicateSSAccQCWriter;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.DUPLICATE_SS_ACC_QC_WRITER;


@Configuration
public class DuplicateSSAccQCWriterConfiguration {
    @Bean(DUPLICATE_SS_ACC_QC_WRITER)
    @StepScope
    DuplicateSSAccQCWriter duplicateSSAccQCWrite(InputParameters parameters) {
        return new DuplicateSSAccQCWriter(parameters.getDuplicateSSAccFile());
    }
}
