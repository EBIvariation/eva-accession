package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.junit.After;
import org.junit.Ignore;
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

import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp.test.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.test.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.dbsnp.test.TestConfiguration;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class, TestConfiguration.class})
@TestPropertySource("classpath:application.properties")
public class ImportDbsnpVariantsStepConfigurationTest {

    private static final int EXPECTED_VARIANTS = 14;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private SubmittedVariantAccessioningRepository repository;

    @Autowired
    private InputParameters inputParameters;

    @After
    public void tearDown() throws Exception {
    }

    @Test
    @DirtiesContext
    @Ignore("In order to make this test pass, we have to implement DbsnpVariantsWriter")
    public void executeStep() throws IOException {

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(IMPORT_DBSNP_VARIANTS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        long numVariantsInDatabase = repository.count();
        assertEquals(EXPECTED_VARIANTS, numVariantsInDatabase);
    }
}
