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

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;

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
    public void assertStepExecutesAndCompletes() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("CONTIG_STEP");
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void contigsWritten() throws Exception {
        assertStepExecutesAndCompletes();
        Path folder = Paths.get(inputParameters.getOutputVcf()).getParent();
        assertEquals(6, numberOfLines(folder + "/contigs_" + inputParameters.getAssemblyAccession() + ".txt"));
    }

    private long numberOfLines(String path) throws IOException {
        BufferedReader br = new BufferedReader(new FileReader(path));
        return br.lines().count();
    }
}