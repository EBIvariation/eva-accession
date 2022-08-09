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
package uk.ac.ebi.eva.accession.release.configuration.batch.jobs;

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
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_DBSNP_ACTIVE_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_DBSNP_MERGED_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_DBSNP_MULTIMAP_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_EVA_ACTIVE_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_EVA_MERGED_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.LIST_EVA_MULTIMAP_CONTIGS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_DBSNP_MULTIMAP_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_EVA_MAPPED_ACTIVE_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_EVA_MAPPED_DEPRECATED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_EVA_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_EVA_MAPPED_MERGED_VARIANTS_STEP;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_EVA_MULTIMAP_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantOperationEntity.json",
        "/test-data/clusteredVariantEntity.json",
        "/test-data/clusteredVariantOperationEntity.json",
        "/test-data/submittedVariantEntity.json",
        "/test-data/submittedVariantOperationEntity.json",
})
@TestPropertySource("classpath:application.properties")
public class AccessionReleaseJobConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final long EXPECTED_LINES = 6;

    private static final long EXPECTED_EVA_LINES = 2;

    private static final long EXPECTED_LINES_MERGED = 5;

    private static final long EXPECTED_EVA_LINES_MERGED = 2;

    private static final long EXPECTED_LINES_DEPRECATED = 3;

    private static final long EXPECTED_EVA_LINES_DEPRECATED = 2;

    private static final long EXPECTED_LINES_MERGED_DEPRECATED = 2;

    private static final long EXPECTED_EVA_LINES_MERGED_DEPRECATED = 1;

    private static final long EXPECTED_LINES_MULTIMAP = 2;

    private static final long EXPECTED_EVA_LINES_MULTIMAP = 1;

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

        List<String> expectedSteps = Arrays.asList(LIST_DBSNP_ACTIVE_CONTIGS_STEP, LIST_DBSNP_MERGED_CONTIGS_STEP,
                                                   LIST_DBSNP_MULTIMAP_CONTIGS_STEP,
                                                   RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP,
                                                   RELEASE_DBSNP_MAPPED_MERGED_VARIANTS_STEP,
                                                   RELEASE_DBSNP_MAPPED_DEPRECATED_VARIANTS_STEP,
                                                   RELEASE_DBSNP_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP,
                                                   RELEASE_DBSNP_MULTIMAP_VARIANTS_STEP,
                                                   LIST_EVA_ACTIVE_CONTIGS_STEP, LIST_EVA_MERGED_CONTIGS_STEP,
                                                   LIST_EVA_MULTIMAP_CONTIGS_STEP,
                                                   RELEASE_EVA_MAPPED_ACTIVE_VARIANTS_STEP,
                                                   RELEASE_EVA_MAPPED_MERGED_VARIANTS_STEP,
                                                   RELEASE_EVA_MAPPED_DEPRECATED_VARIANTS_STEP,
                                                   RELEASE_EVA_MAPPED_MERGED_DEPRECATED_VARIANTS_STEP,
                                                   RELEASE_EVA_MULTIMAP_VARIANTS_STEP);
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
        long numVariantsInMergedDeprecatedRelease = FileUtils.countNonCommentLines(getMergedDeprecatedRelease());
        assertEquals(EXPECTED_LINES_MERGED_DEPRECATED, numVariantsInMergedDeprecatedRelease);
        long numVariantsInMultimapRelease = FileUtils.countNonCommentLines(getMultimapRelease());
        assertEquals(EXPECTED_LINES_MULTIMAP, numVariantsInMultimapRelease);

        long numVariantsInEvaRelease = FileUtils.countNonCommentLines(getEvaRelease());
        assertEquals(EXPECTED_EVA_LINES, numVariantsInEvaRelease);
        long numVariantsInEvaMergedRelease = FileUtils.countNonCommentLines(getEvaMergedRelease());
        assertEquals(EXPECTED_EVA_LINES_MERGED, numVariantsInEvaMergedRelease);
        long numVariantsInEvaDeprecatedRelease = FileUtils.countNonCommentLines(getEvaDeprecatedRelease());
        assertEquals(EXPECTED_EVA_LINES_DEPRECATED, numVariantsInEvaDeprecatedRelease);
        long numVariantsInEvaMergedDeprecatedRelease = FileUtils.countNonCommentLines(getEvaMergedDeprecatedRelease());
        assertEquals(EXPECTED_EVA_LINES_MERGED_DEPRECATED, numVariantsInEvaMergedDeprecatedRelease);
        long numVariantsInEvaMultimapRelease = FileUtils.countNonCommentLines(getEvaMultimapRelease());
        assertEquals(EXPECTED_EVA_LINES_MULTIMAP, numVariantsInEvaMultimapRelease);
    }

    private FileInputStream getRelease() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getDbsnpCurrentIdsReportPath(inputParameters.getOutputFolder(),
                                                                                   inputParameters.getAssemblyAccession())
                                                     .toFile());
    }

    private FileInputStream getEvaRelease() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getEvaCurrentIdsReportPath(inputParameters.getOutputFolder(),
                                                                                 inputParameters.getAssemblyAccession())
                                                     .toFile());
    }

    private FileInputStream getMergedRelease() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getDbsnpMergedIdsReportPath(inputParameters.getOutputFolder(),
                                                                                  inputParameters.getAssemblyAccession())
                                                     .toFile());
    }

    private FileInputStream getEvaMergedRelease() throws FileNotFoundException {
        return new FileInputStream(ReportPathResolver.getEvaMergedIdsReportPath(inputParameters.getOutputFolder(),
                                                                                inputParameters.getAssemblyAccession())
                                                     .toFile());
    }

    private FileInputStream getDeprecatedRelease() throws FileNotFoundException {
        return new FileInputStream(
                ReportPathResolver.getDbsnpDeprecatedIdsReportPath(inputParameters.getOutputFolder(),
                                                                   inputParameters.getAssemblyAccession())
                                  .toFile());
    }

    private FileInputStream getEvaDeprecatedRelease() throws FileNotFoundException {
        return new FileInputStream(
                ReportPathResolver.getEvaDeprecatedIdsReportPath(inputParameters.getOutputFolder(),
                                                                 inputParameters.getAssemblyAccession())
                                  .toFile());
    }

    private FileInputStream getMergedDeprecatedRelease() throws FileNotFoundException {
        return new FileInputStream(
                ReportPathResolver.getDbsnpMergedDeprecatedIdsReportPath(inputParameters.getOutputFolder(),
                                                                         inputParameters.getAssemblyAccession()).toFile());
    }

    private FileInputStream getEvaMergedDeprecatedRelease() throws FileNotFoundException {
        return new FileInputStream(
                ReportPathResolver.getEvaMergedDeprecatedIdsReportPath(inputParameters.getOutputFolder(),
                                                                       inputParameters.getAssemblyAccession()).toFile());
    }

    private FileInputStream getMultimapRelease() throws FileNotFoundException {
        return new FileInputStream(
                ReportPathResolver.getDbsnpMultimapIdsReportPath(inputParameters.getOutputFolder(),
                                                                 inputParameters.getAssemblyAccession()).toFile());
    }

    private FileInputStream getEvaMultimapRelease() throws FileNotFoundException {
        return new FileInputStream(
                ReportPathResolver.getEvaMultimapIdsReportPath(inputParameters.getOutputFolder(),
                                                               inputParameters.getAssemblyAccession()).toFile());
    }

    private void assertStepsExecuted(List<String> expectedSteps, JobExecution jobExecution) {
        Collection<StepExecution> stepExecutions = jobExecution.getStepExecutions();
        List<String> steps = stepExecutions.stream().map(StepExecution::getStepName).collect(Collectors.toList());
        assertEquals(expectedSteps, steps);
    }

}


