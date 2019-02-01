package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.io.ContigWriter;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import java.io.File;
import java.nio.file.Paths;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_WRITER;

@Configuration
public class ContigWriterConfiguration {

    @Bean(CONTIG_WRITER)
    public ContigWriter variantContextWriter(InputParameters parameters) {
        String path = Paths.get(parameters.getOutputVcf()).getParent().toString();
        return new ContigWriter(new File(path + "/contigs_" + parameters.getAssemblyAccession() + ".txt"));
    }
}
