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
import java.util.List;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@TestPropertySource("classpath:application.properties")
public class CreateDeprecatedReleaseStepConfigurationTest {

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
    public void basicStepCompletion() {
        assertStepExecutesAndCompletes();
    }

    private void assertStepExecutesAndCompletes() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void variantsWritten() throws Exception {
        assertStepExecutesAndCompletes();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(getDeprecatedReleaseFile()));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
    }

    private File getDeprecatedReleaseFile() {
        return ReportPathResolver.getDbsnpDeprecatedIdsReportPath(inputParameters.getOutputFolder(),
                                                                  inputParameters.getAssemblyAccession()).toFile();
    }

    @Test
    public void accessionsWritten() throws Exception {
        assertStepExecutesAndCompletes();
        long numVariantsInRelease = FileUtils.countNonCommentLines(new FileInputStream(getDeprecatedReleaseFile()));
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
        List<String> dataLinesWithRs = grepFile(getDeprecatedReleaseFile(), "^rs[0-9]+$");
        assertEquals(EXPECTED_LINES, dataLinesWithRs.size());
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

}
