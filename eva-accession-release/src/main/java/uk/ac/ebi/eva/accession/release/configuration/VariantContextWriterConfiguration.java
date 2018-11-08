package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.io.VariantContextWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import java.io.File;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_WRITER;

@Configuration
public class VariantContextWriterConfiguration {

    @Bean(RELEASE_WRITER)
    public VariantContextWriter variantContextWriter(InputParameters parameters) {
        return new VariantContextWriter(new File(parameters.getOutputVcf()), parameters.getAssemblyAccession());
    }

}
