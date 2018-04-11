package uk.ac.ebi.eva.accession.pipeline.configuration.jobs.steps;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.pipeline.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {InputParametersConfiguration.class, BatchTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
@DataJpaTest
public class CreateSubsnpAccessionsStepConfigurationTest {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    @Qualifier("JOB-LAUNCHER")
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Test
    public void executeStep() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("CREATE_SUBSNP_ACCESSION_STEP");
        assertEquals(jobExecution.getStatus(), BatchStatus.COMPLETED);
    }
}
