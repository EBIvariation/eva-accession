package uk.ac.ebi.eva.accession.pipeline.configuration.jobs.steps;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;

import java.util.Collections;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class CreateSubsnpAccessionsStepConfigurationTest {

    private static final long EXPECTED_ACCESSION = 10000000000L;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private SubmittedVariantAccessioningService service;

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Test
    public void executeStep() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep("CREATE_SUBSNP_ACCESSION_STEP");
        assertEquals(jobExecution.getStatus(), BatchStatus.COMPLETED);

        Map<Long, ISubmittedVariant> retrievedVariants = service.getByAccessions(
                Collections.singletonList(EXPECTED_ACCESSION));

        long variants = repository.count();
        assertNotEquals(0, variants);
    }
}
