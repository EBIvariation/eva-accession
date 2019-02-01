package uk.ac.ebi.eva.accession.release.configuration;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.TestConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, TestConfiguration.class})
@TestPropertySource("classpath:application.properties")
public class ContigStepConfigurationTest {

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private InputParameters inputParameters;

    @Test
    @DirtiesContext
    public void executeStep() throws IOException {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("CONTIG_STEP");
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

//        assertEquals(1, jobExecution.getStepExecutions().size());
//        assertEquals(EXPECTED_PROCESSED_CONTIGS_COUNT,
//                     jobExecution.getStepExecutions().iterator().next().getWriteCount());
    }

}