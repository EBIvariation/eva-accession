package uk.ac.ebi.eva.accession.pipeline.configuration.jobs;

import org.junit.After;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.MongoTestConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CHECK_SUBSNP_ACCESSION_STEP;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, SubmittedVariantAccessioningConfiguration.class,
        MongoTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class CreateSubsnpAccessionsJobConfigurationTest {

    private static final int EXPECTED_VARIANTS = 299;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private InputParameters inputParameters;

    @After
    public void tearDown() throws Exception {
        new File(inputParameters.getOutputVcf()).delete();
    }

    @Test
    @DirtiesContext
    public void executeJob() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        assertStepNames(jobExecution.getStepExecutions());

        long numVariantsInDatabase = repository.count();
        assertEquals(EXPECTED_VARIANTS, numVariantsInDatabase);

        long numVariantsInReport = FileUtils.countNonCommentLines(new FileInputStream(inputParameters.getOutputVcf()));
        assertEquals(EXPECTED_VARIANTS, numVariantsInReport);
    }

    private void assertStepNames(Collection<StepExecution> stepExecutions) {
        assertEquals(2, stepExecutions.size());
        Iterator<StepExecution> iterator = stepExecutions.iterator();
        assertEquals(CREATE_SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
        assertEquals(CHECK_SUBSNP_ACCESSION_STEP, iterator.next().getStepName());
    }
}
