/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;
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
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_MAPPED_MERGED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.MERGED_INTO_KEY;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.STUDY_ID_KEY;
import static uk.ac.ebi.eva.accession.release.io.MergedVariantMongoReader.VARIANT_CLASS_KEY;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@TestPropertySource("classpath:application.properties")
public class CreateMergedReleaseStepConfigurationTest {

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
    public void basicStepCompletion() throws Exception {
        assertStepExecutesAndCompletes();
    }

    private void assertStepExecutesAndCompletes() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(RELEASE_MAPPED_MERGED_VARIANTS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void variantsWritten() throws Exception {
        assertStepExecutesAndCompletes();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(getMergedReleaseFile()));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
    }

    private File getMergedReleaseFile() {
        return ReportPathResolver.getMergedIdsReportPath(inputParameters.getOutputFolder(),
                                                         inputParameters.getAssemblyAccession()).toFile();
    }

    @Test
    public void metadataIsPresent() throws Exception {
        assertStepExecutesAndCompletes();

        List<String> referenceLines = grepFile(getMergedReleaseFile(),
                                               "^##reference=" + inputParameters.getAssemblyAccession() + "$");
        assertEquals(1, referenceLines.size());

        List<String> metadataVariantClassLines = grepFile(getMergedReleaseFile(),
                                                          "^##INFO=<ID=" + VARIANT_CLASS_KEY + ",.*$");
        assertEquals(1, metadataVariantClassLines.size());

        List<String> metadataStudyIdLines = grepFile(getMergedReleaseFile(),
                                                     "^##INFO=<ID=" + STUDY_ID_KEY + ",.*$");
        assertEquals(1, metadataStudyIdLines.size());

        List<String> metadataMergedIntoLines = grepFile(getMergedReleaseFile(),
                                                        "^##INFO=<ID=" + MERGED_INTO_KEY + ",.*$");
        assertEquals(1, metadataMergedIntoLines.size());

        List<String> headerLines = grepFile(getMergedReleaseFile(),
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
        assertStepExecutesAndCompletes();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(getMergedReleaseFile()));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
        String column = "[^\t]+\t";
        List<String> dataLinesWithRs = grepFile(getMergedReleaseFile(),
                                                "^" + column + column +"rs[0-9]+\t.*$");
        assertEquals(3, dataLinesWithRs.size());
    }

    @Test
    public void infoWritten() throws Exception {
        assertStepExecutesAndCompletes();
        File outputFile = getMergedReleaseFile();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(outputFile));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
        String dataLinesDoNotStartWithHash = "^[^#]";
        String variantClass = VARIANT_CLASS_KEY + "=SO:[0-9]+";
        String studyId = STUDY_ID_KEY + "=[a-zA-Z0-9,]+";
        String mergedInto = MERGED_INTO_KEY + "=rs[0-9]+";

        List<String> dataLines;
        dataLines = grepFile(outputFile, dataLinesDoNotStartWithHash + ".*" + variantClass + ".*");
        assertEquals(EXPECTED_LINES, dataLines.size());
        dataLines = grepFile(outputFile, dataLinesDoNotStartWithHash + ".*" + studyId + ".*");
        assertEquals(EXPECTED_LINES, dataLines.size());
        dataLines = grepFile(outputFile, dataLinesDoNotStartWithHash + ".*" + mergedInto + ".*");
        assertEquals(EXPECTED_LINES, dataLines.size());
    }
}
