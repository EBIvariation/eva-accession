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

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;
import uk.ac.ebi.eva.remapping.source.configuration.BeanNames;
import uk.ac.ebi.eva.remapping.source.parameters.InputParameters;
import uk.ac.ebi.eva.remapping.source.parameters.ReportPathResolver;
import uk.ac.ebi.eva.remapping.source.test.configuration.BatchTestConfiguration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.remapping.source.test.configuration.BatchTestConfiguration.JOB_EXPORT_SUBMITTED_VARIANTS_JOB;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:with-projects.properties")
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
public class ExportSubmittedVariantsJobConfigurationTest extends MongoTestContainerHelper {
    private static final long EXPECTED_LINES_DBSNP = 3;

    private static final long EXPECTED_LINES_EVA = 1;

    @Autowired
    @Qualifier(JOB_EXPORT_SUBMITTED_VARIANTS_JOB)
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate.getDb().drop();
        deleteOutputFiles();

        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load("/test-data/dbsnpSubmittedVariantEntity.json");
        mongoTestDataLoader.load("/test-data/submittedVariantEntity.json");
    }

    @AfterEach
    public void tearDown() throws Exception {
        mongoTemplate.getDb().drop();
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