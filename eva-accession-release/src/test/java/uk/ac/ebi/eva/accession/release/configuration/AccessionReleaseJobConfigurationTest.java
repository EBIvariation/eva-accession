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
package uk.ac.ebi.eva.accession.release.configuration;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
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

import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;
import uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_MAPPED_ACTIVE_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_MAPPED_DEPRECATED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_MAPPED_MERGED_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"})
@TestPropertySource("classpath:application.properties")
public class AccessionReleaseJobConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final long EXPECTED_LINES = 3;

    private static final long EXPECTED_LINES_MERGED = 3;

    private static final long EXPECTED_LINES_DEPRECATED = 1;

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
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();

        List<String> expectedSteps = Arrays.asList(LIST_CONTIGS_STEP, RELEASE_MAPPED_ACTIVE_VARIANTS_STEP,
                                                   RELEASE_MAPPED_MERGED_VARIANTS_STEP,
                                                   RELEASE_MAPPED_DEPRECATED_VARIANTS_STEP);
        assertStepsExecuted(expectedSteps, jobExecution);

        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void variantsWritten() throws Exception {
        JobExecution jobExecution = jobLauncherTestUtils.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        long numVariantsInRelease = FileUtils.countNonCommentLines(getRelease());
        assertEquals(EXPECTED_LINES, numVariantsInRelease);
        long numVariantsInMergedRelease = FileUtils.countNonCommentLines(getMergedRelease());
        assertEquals(EXPECTED_LINES_MERGED, numVariantsInMergedRelease);
        long numVariantsInDeprecatedRelease = FileUtils.countNonCommentLines(getDeprecatedRelease());
        assertEquals(EXPECTED_LINES_DEPRECATED, numVariantsInDeprecatedRelease);
    }

    private FileInputStream getRelease() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getCurrentIdsReportPath(
                inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession()).toFile());
    }

    private FileInputStream getMergedRelease() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getMergedIdsReportPath(
                inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession()).toFile());
    }

    private FileInputStream getDeprecatedRelease() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getDeprecatedIdsReportPath(
                inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession()).toFile());
    }

    private void assertStepsExecuted(List expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

}
