/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.source.configuration.batch.jobs;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.remapping.source.parameters.InputParameters;
import uk.ac.ebi.eva.remapping.source.parameters.ReportPathResolver;
import uk.ac.ebi.eva.remapping.source.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.remapping.source.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.remapping.source.configuration.BeanNames;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/submittedVariantEntity.json"})
@TestPropertySource("classpath:with-projects.properties")
public class ExportSubmittedVariantsJobConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final long EXPECTED_LINES_DBSNP = 3;

    private static final long EXPECTED_LINES_EVA = 1;

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

    @Before
    public void setUp() throws Exception {
        deleteOutputFiles();
    }

    @After
    public void tearDown() throws Exception {
        deleteOutputFiles();
    }

    private void deleteOutputFiles() {
        ReportPathResolver.getDbsnpReportPath(inputParameters.getOutputFolder(),
                                              inputParameters.getAssemblyAccession(),
                                              inputParameters.getTaxonomy())
                          .toFile().delete();
        ReportPathResolver.getEvaReportPath(inputParameters.getOutputFolder(),
                                            inputParameters.getAssemblyAccession(),
                                            inputParameters.getTaxonomy())
                          .toFile().delete();
    }

    @Test
    public void contextLoads() {

    }

    @Test
    public void basicJobCompletion() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        List<String> expectedSteps = Arrays.asList(BeanNames.EXPORT_EVA_SUBMITTED_VARIANTS_STEP,
                                                   BeanNames.EXPORT_DBSNP_SUBMITTED_VARIANTS_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void variantsWritten() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        long numVariantsInRelease = FileUtils.countNonCommentLines(getExportedEva());
        assertEquals(EXPECTED_LINES_EVA, numVariantsInRelease);
        long numVariantsInMergedRelease = FileUtils.countNonCommentLines(getExportedDbsnp());
        assertEquals(EXPECTED_LINES_DBSNP, numVariantsInMergedRelease);
    }

    private FileInputStream getExportedEva() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getEvaReportPath(inputParameters.getOutputFolder(),
                                                                       inputParameters.getAssemblyAccession(),
                                                                       inputParameters.getTaxonomy())
                                                     .toFile());
    }

    private FileInputStream getExportedDbsnp() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getDbsnpReportPath(inputParameters.getOutputFolder(),
                                                                         inputParameters.getAssemblyAccession(),
                                                                         inputParameters.getTaxonomy())
                                                     .toFile());
    }

    private void assertStepsExecuted(List expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

}