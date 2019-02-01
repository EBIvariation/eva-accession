package uk.ac.ebi.eva.accession.release.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.configuration.DbsnpDataSource;
import uk.ac.ebi.eva.accession.core.io.SubSnpNoHgvsContigReader;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;

import javax.sql.DataSource;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CONTIG_READER;

@Configuration
@EnableConfigurationProperties({DbsnpDataSource.class})
public class ContigReaderConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ContigReaderConfiguration.class);

    @Bean(name = CONTIG_READER)
    @StepScope
    SubSnpNoHgvsContigReader contigsReader(InputParameters parameters, DbsnpDataSource dbsnpDataSource)
            throws Exception {
        logger.info("Injecting ContigsReaderConfiguration with parameters: {}, {}", parameters, dbsnpDataSource);
        DataSource dataSource = dbsnpDataSource.getDatasource();
        return new SubSnpNoHgvsContigReader(parameters.getAssemblyName(), parameters.getBuildNumber(), dataSource,
                                            parameters.getPageSize());
    }
}
