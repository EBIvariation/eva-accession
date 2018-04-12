package uk.ac.ebi.eva.accession.pipeline.configuration;

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
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.core.io.ResourceLoader;
import org.springframework.jdbc.datasource.init.DatabasePopulatorUtils;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.transaction.PlatformTransactionManager;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;

@Configuration
@AutoConfigureDataJpa
@EnableBatchProcessing
@EnableConfigurationProperties(value=BatchProperties.class)
@ComponentScan(basePackages = {"uk.ac.ebi.eva.accession"})
public class BatchTestConfiguration {

    @Bean("JOB-LAUNCHER")
    public JobLauncherTestUtils jobLauncherTestUtils() {
        return new JobLauncherTestUtils();
    }

    @Autowired
    private BatchProperties properties;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private DataSource dataSource;

    @Autowired
    private PlatformTransactionManager platformTransactionManager;

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

    @Bean
    public JobRepository jobRepository() throws Exception {
        JobRepositoryFactoryBean jobRepositoryFactoryBean = new JobRepositoryFactoryBean();
        jobRepositoryFactoryBean.setDataSource(dataSource);
        jobRepositoryFactoryBean.setTransactionManager(platformTransactionManager);
        jobRepositoryFactoryBean.setDatabaseType("h2");
        return jobRepositoryFactoryBean.getObject();
    }

    /*@Bean("TRANSACTION_MANAGER")
    public PlatformTransactionManager inMemoryTransactionManager() {
        return new ResourcelessTransactionManager();
    }*/

    /*@Bean
    @Primary
    public DataSource inMemoryDatasource() {
        DataSourceBuilder dataSourceBuilder = DataSourceBuilder.create();
        dataSourceBuilder.driverClassName("org.h2.Driver");
        dataSourceBuilder.url("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;DB_CLOSE_ON_EXIT=FALSE");
        dataSourceBuilder.username("sa");
        dataSourceBuilder.password("");
        return dataSourceBuilder.build();
    }*/
}