package uk.ac.ebi.eva.accession.pipeline.test;

import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.autoconfigure.batch.BatchProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
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

@EnableAutoConfiguration
@Import({CreateSubsnpAccessionsJobConfiguration.class, CreateSubsnpAccessionsStepConfiguration.class,
        VcfReaderConfiguration.class, VariantProcessorConfiguration.class, AccessionWriterConfiguration.class,
        ChunkSizeCompletionPolicyConfiguration.class})
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