package uk.ac.ebi.eva.accession.pipeline.test;

import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.repository.support.JobRepositoryFactoryBean;
import org.springframework.batch.support.transaction.ResourcelessTransactionManager;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.boot.test.autoconfigure.orm.jpa.AutoConfigureDataJpa;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;

import uk.ac.ebi.eva.accession.pipeline.configuration.AccessionWriterConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.ChunkSizeCompletionPolicyConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.VariantProcessorConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.VcfReaderConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.jobs.CreateSubsnpAccessionsJobConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.jobs.steps.CreateSubsnpAccessionsStepConfiguration;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

@Configuration
@AutoConfigureDataJpa
@EnableBatchProcessing
@EnableConfigurationProperties(value=BatchProperties.class)
@Import({CreateSubsnpAccessionsJobConfiguration.class, CreateSubsnpAccessionsStepConfiguration.class,
        VcfReaderConfiguration.class, VariantProcessorConfiguration.class, AccessionWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
//@ComponentScan(basePackages = {"uk.ac.ebi.eva.accession"})
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

    @PostConstruct
    protected void initialize() {
        if (this.properties.getInitializer().isEnabled()) {
            String platform = "h2";
            ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
            String schemaLocation = this.properties.getSchema();
            schemaLocation = schemaLocation.replace("@@platform@@", platform);
            populator.addScript(this.resourceLoader.getResource(schemaLocation));
            populator.setContinueOnError(true);
            DatabasePopulatorUtils.execute(populator, dataSource);
        }
    }
}