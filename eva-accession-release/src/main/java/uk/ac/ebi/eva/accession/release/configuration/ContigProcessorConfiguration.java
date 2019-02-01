package uk.ac.ebi.eva.accession.release.configuration;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.steps.processors.ContigProcessor;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_PROCESSOR;

@Configuration
public class ContigProcessorConfiguration {

    @Bean(name = CONTIG_PROCESSOR)
    public ContigProcessor contigProcessor(ContigMapping contigMapping) {
        return new ContigProcessor(contigMapping);
    }

    @Bean
    ContigMapping contigMapping(InputParameters parameters) throws Exception {
        return new ContigMapping(parameters.getAssemblyReportUrl());
    }
}
