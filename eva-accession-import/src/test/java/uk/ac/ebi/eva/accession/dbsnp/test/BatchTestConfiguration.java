package uk.ac.ebi.eva.accession.dbsnp.test;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.core.io.ResourceLoader;
import org.springframework.transaction.PlatformTransactionManager;

import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.ImportDbsnpVariantsJobConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.ImportDbsnpVariantsProcessorConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.ImportDbsnpVariantsReaderConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.ImportDbsnpVariantsStepConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.ImportDbsnpVariantsWriterConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.configuration.InputParametersConfiguration;

import javax.sql.DataSource;

@EnableAutoConfiguration
@Import({DbsnpTestDataSource.class,
        MongoConfiguration.class,
        ImportDbsnpVariantsJobConfiguration.class,
        ImportDbsnpVariantsStepConfiguration.class,
        ImportDbsnpVariantsReaderConfiguration.class,
        ImportDbsnpVariantsProcessorConfiguration.class,
        ImportDbsnpVariantsWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class,
        InputParametersConfiguration.class})
public class BatchTestConfiguration {

    @Autowired
    private BatchProperties properties;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

    @Bean
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }

}
