package uk.ac.ebi.eva.accession.release.configuration;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CREATE_RELEASE_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@TestPropertySource("classpath:application.properties")
public class CreateReleaseStepConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final long EXPECTED_LINES = 3;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private InputParameters inputParameters;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Test
    public void contextLoads() {

    }

    @Test
    public void basicJobCompletion() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(CREATE_RELEASE_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void variantsWritten() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(CREATE_RELEASE_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(inputParameters.getOutputVcf()));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
    }

    @Test
    public void metadataIsPresent() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(CREATE_RELEASE_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        List<String> referenceLines = grepFile(new File(inputParameters.getOutputVcf()),
                                               "^##reference=" + inputParameters.getAssemblyAccession() + "$");
        assertEquals(1, referenceLines.size());

        List<String> metadataVariantClassLines = grepFile(new File(inputParameters.getOutputVcf()),
                                                          "^##INFO=<ID=VC.*$");
        assertEquals(1, metadataVariantClassLines.size());

        List<String> metadataStudyIdLines = grepFile(new File(inputParameters.getOutputVcf()), "^##INFO=<ID=SID.*$");
        assertEquals(1, metadataStudyIdLines.size());

        List<String> headerLines = grepFile(new File(inputParameters.getOutputVcf()),
                                                     "^#CHROM\tPOS\tID\tREF\tALT\tQUAL\tFILTER\tINFO$");
        assertEquals(1, headerLines.size());
    }

    private List<String> grepFile(File file, String regex) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(file)));
        List<String> lines = new ArrayList<>();
        String line;
        while ((line = reader.readLine()) != null) {
            if (line.matches(regex)) {
                lines.add(line);
            }
        }
        reader.close();
        return lines;
    }

    @Test
    public void rsAccessionsWritten() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(CREATE_RELEASE_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(inputParameters.getOutputVcf()));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
        List<String> dataLinesWithRs = grepFile(new File(inputParameters.getOutputVcf()), "^.*\trs[0-9]+\t.*$");
        assertEquals(3, dataLinesWithRs.size());
    }

    @Test
    public void infoWritten() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(CREATE_RELEASE_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(inputParameters.getOutputVcf()));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
        String dataLinesDoNotStartWithHash = "^[^#]";
        String sevenColumns = "([^\t]+\t){7}";
        String variantClass = "VC=SO:[0-9]+";
        String studyId = "SID=[a-zA-Z0-9,]+";
        String variantClassAndStudyId = variantClass + ";" + studyId;
        String studyIdAndVariantClass = studyId + ";" + variantClass;
        String variantClassAndStudyIdInAnyOrder = "(" + variantClassAndStudyId + "|" + studyIdAndVariantClass + ")";
        List<String> dataLinesWithVariantClassAndStudyId = grepFile(new File(inputParameters.getOutputVcf()),
                                                                    dataLinesDoNotStartWithHash + sevenColumns
                                                                            + variantClassAndStudyIdInAnyOrder + "$");

        assertEquals(3, dataLinesWithVariantClassAndStudyId.size());
    }
}
