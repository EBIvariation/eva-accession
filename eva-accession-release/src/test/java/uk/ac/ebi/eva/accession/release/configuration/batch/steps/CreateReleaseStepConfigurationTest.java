/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

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
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.STUDY_ID_KEY;
import static uk.ac.ebi.eva.accession.release.batch.io.active.AccessionedVariantMongoReader.VARIANT_CLASS_KEY;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_EVA_MAPPED_ACTIVE_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/clusteredVariantEntity.json",
        "/test-data/submittedVariantEntity.json"})
@TestPropertySource("classpath:application.properties")
public class CreateReleaseStepConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final long EXPECTED_DBSNP_LINES = 6;

    private static final long EXPECTED_EVA_LINES = 2;

    private static final Map<String, String> assemblyAccessionToName =
            Collections.singletonMap("GCA_000409795.2", "Chlorocebus_sabeus 1.1");

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
    public void basicStepCompletion() {
        assertStepExecutesAndCompletes();
    }

    private void assertStepExecutesAndCompletes() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void variantsWritten() throws Exception {
        assertStepExecutesAndCompletes();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(getReleaseFile()));
        assertEquals(EXPECTED_DBSNP_LINES, numVariantsInRelease);
    }

    private File getReleaseFile() {
        return ReportPathResolver.getDbsnpCurrentIdsReportPath(inputParameters.getOutputFolder(),
                                                               inputParameters.getAssemblyAccession()).toFile();
    }

    @Test
    public void metadataIsPresent() throws Exception {
        assertStepExecutesAndCompletes();

        String assemblyName = assemblyAccessionToName.get(inputParameters.getAssemblyAccession());
        assertNotNull(assemblyName);
        List<String> referenceLines = grepFile(getReleaseFile(), "^##reference=<ID=" + assemblyName + ",.*$");
        assertEquals(1, referenceLines.size());

        List<String> metadataVariantClassLines = grepFile(getReleaseFile(),
                                                          "^##INFO=<ID=" + VARIANT_CLASS_KEY + ".*$");
        assertEquals(1, metadataVariantClassLines.size());

        List<String> metadataStudyIdLines = grepFile(getReleaseFile(),
                                                     "^##INFO=<ID=" + STUDY_ID_KEY + ".*$");
        assertEquals(1, metadataStudyIdLines.size());

        List<String> headerLines = grepFile(getReleaseFile(),
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
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(getReleaseFile()));
        assertEquals(EXPECTED_DBSNP_LINES, numVariantsInRelease);
        List<String> dataLinesWithRs = grepFile(getReleaseFile(), "^.*\trs[0-9]+\t.*$");
        assertEquals(EXPECTED_DBSNP_LINES, dataLinesWithRs.size());
    }

    @Test
    public void infoWritten() throws Exception {
        assertStepExecutesAndCompletes();
        File outputFile = getReleaseFile();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(outputFile));
        assertEquals(EXPECTED_DBSNP_LINES, numVariantsInRelease);
        String dataLinesDoNotStartWithHash = "^[^#]";
        String variantClass = VARIANT_CLASS_KEY + "=SO:[0-9]+";
        String studyId = STUDY_ID_KEY + "=[a-zA-Z0-9,]+";

        List<String> dataLines;
        dataLines = grepFile(outputFile, dataLinesDoNotStartWithHash + ".*" + variantClass + ".*");
        assertEquals(EXPECTED_DBSNP_LINES, dataLines.size());
        dataLines = grepFile(outputFile, dataLinesDoNotStartWithHash + ".*" + studyId + ".*");
        assertEquals(EXPECTED_DBSNP_LINES, dataLines.size());

    }

    /**
     * Variant rs8181 is an insertion and when retrieving the context nucleotide from the FASTA it brings a Y which is
     * invalid in VCF. We have to make sure variants like that one are excluded before we write the VCF file.
     */
    @Test
    public void excludeInvalidVariants() throws IOException {
        assertStepExecutesAndCompletes();
        File outputFile = getReleaseFile();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(outputFile));
        assertEquals(EXPECTED_DBSNP_LINES, numVariantsInRelease);
        assertEquals(0, grepFile(outputFile, ".*rs8181.*").size());
        assertEquals(1, grepFile(outputFile, ".*rs869927931.*REMAPPED.*").size());
    }

    @Test
    public void evaVariantsWritten() throws Exception {
        assertEvaStepExecutesAndCompletes();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(getEvaReleaseFile()));
        assertEquals(EXPECTED_EVA_LINES, numVariantsInRelease);
    }

    private void assertEvaStepExecutesAndCompletes() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(RELEASE_EVA_MAPPED_ACTIVE_VARIANTS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    private File getEvaReleaseFile() {
        return ReportPathResolver.getEvaCurrentIdsReportPath(inputParameters.getOutputFolder(),
                                                             inputParameters.getAssemblyAccession()).toFile();
    }
}
