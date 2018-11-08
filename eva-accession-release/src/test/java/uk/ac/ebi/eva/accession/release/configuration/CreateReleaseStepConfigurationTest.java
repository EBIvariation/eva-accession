package uk.ac.ebi.eva.accession.release.configuration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@TestPropertySource("classpath:application.properties")
public class CreateReleaseStepConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private InputParameters inputParameters;

    @Test
    public void contextLoads() {

    }

    @Test
    public void basicJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }
}
