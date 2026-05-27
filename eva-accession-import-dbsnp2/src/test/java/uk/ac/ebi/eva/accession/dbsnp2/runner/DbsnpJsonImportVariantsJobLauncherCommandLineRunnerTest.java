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
package uk.ac.ebi.eva.accession.dbsnp2.runner;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.dbsnp2.batch.io.BzipLazyResource;
import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp2.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.IMPORT_DBSNP_JSON_VARIANTS_JOB;
import static uk.ac.ebi.eva.accession.dbsnp2.runner.DbsnpJsonImportVariantsJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@TestPropertySource("classpath:application.properties")
@SpringBatchTest
public class DbsnpJsonImportVariantsJobLauncherCommandLineRunnerTest extends MongoTestContainerHelper {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private DbsnpJsonImportVariantsJobLauncherCommandLineRunner runner;

    @Autowired
    private FlatFileItemReader<JsonNode> variantReader;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private static File tempJsonInputFileToTestFailingJobs;

    private static String originalJsonInputFilePath;

    private static String originalJsonContent;

    private boolean originalInputParametersCaptured = false;

    @BeforeAll
    public static void initializeTempFile() throws Exception {
        tempJsonInputFileToTestFailingJobs = File.createTempFile("resumeFailingJob", ".json.bz2");
    }

    @AfterAll
    public static void deleteTempFile() {
        tempJsonInputFileToTestFailingJobs.delete();
    }

    @BeforeEach
    public void setUp() throws Exception {
        if (!originalInputParametersCaptured) {
            originalJsonInputFilePath = inputParameters.getInput();
            originalJsonContent = getOriginalJsonContent(originalJsonInputFilePath);
            writeToTempJsonFile(originalJsonContent);
            originalInputParametersCaptured = true;
        }
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository);
        runner.setJobName(IMPORT_DBSNP_JSON_VARIANTS_JOB);
        jobRepositoryTestUtils.removeJobExecutions();
        useOriginalJsonFile();
    }

    @AfterEach
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    @DirtiesContext
    public void runSuccessfulJob() throws Exception {
        inputParameters.setForceImport(true);
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void restartCompletedJobThatIsAlreadyInTheRepository() throws Exception {
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());

        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    @DirtiesContext
    public void forceRestartButNoJobInTheRepository() throws Exception {
        assertEquals(Collections.EMPTY_LIST, jobExplorer.getJobNames());
        runner.run();

        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    private void useOriginalJsonFile() throws Exception {
        inputParameters.setInput(originalJsonInputFilePath);
        variantReader.setResource(FileUtils.getResource(new File(originalJsonInputFilePath)));
    }

    private void writeToTempJsonFile(String modifiedJsonContent) throws IOException {
        OutputStream bzipOutputStream =
                new BzipLazyResource(tempJsonInputFileToTestFailingJobs.getAbsolutePath()).getOutputStream();
        bzipOutputStream.write(modifiedJsonContent.getBytes(StandardCharsets.UTF_8));
        bzipOutputStream.close();
    }

    private String getOriginalJsonContent(String inputJsonPath) throws IOException {
        InputStream bzipInputStream = new BzipLazyResource(inputJsonPath).getInputStream();
        BufferedReader reader = new BufferedReader(new InputStreamReader(bzipInputStream));
        StringBuilder originalJsonContent = new StringBuilder();
        String read;
        while ((read = reader.readLine()) != null) {
            originalJsonContent.append(read).append(System.lineSeparator());
        }
        return originalJsonContent.toString();
    }
}