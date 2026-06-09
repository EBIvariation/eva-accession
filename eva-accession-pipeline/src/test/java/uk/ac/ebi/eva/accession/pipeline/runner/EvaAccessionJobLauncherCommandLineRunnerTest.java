/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.ac.ebi.eva.accession.pipeline.runner;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.explore.JobExplorer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.client.ExpectedCount;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.eva.accession.core.configuration.ContiguousIdBlocksDataSourceConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.metrics.count.CountServiceParameters;

import javax.sql.DataSource;
import java.io.File;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.Statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.method;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withStatus;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.SUBSNP_ACCESSION_JOB;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.deleteTemporaryContigAndVariantFiles;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.getOriginalVcfContent;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.useOriginalVcfFile;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.writeToTempVCFFile;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        ContiguousIdBlocksDataSourceConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class EvaAccessionJobLauncherCommandLineRunnerTest extends MongoTestContainerHelper {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private JobExplorer jobExplorer;

    @Autowired
    private EvaAccessionJobLauncherCommandLineRunner runner;

    @Autowired
    private VcfReader vcfReader;

    @Autowired
    private CountServiceParameters countServiceParameters;

    @Autowired
    @Qualifier("COUNT_STATS_REST_TEMPLATE")
    private RestTemplate restTemplate;

    @Autowired
    private MongoTemplate mongoTemplate;

    private final String URL_PATH_SAVE_COUNT = "/v1/bulk/count";

    private MockRestServiceServer mockServer;

    private JobRepositoryTestUtils jobRepositoryTestUtils;

    private static File tempVcfInputFileToTestFailingJobs;

    private static Path tempVcfOutputDir;

    private static String originalVcfInputFilePath;

    private static String originalVcfContent;

    private boolean originalInputParametersCaptured = false;

    @Autowired
    private DataSource dataSource;

    @BeforeAll
    public static void initializeTempFile() throws Exception {
        tempVcfInputFileToTestFailingJobs = File.createTempFile("resumeFailingJob", ".vcf.gz");
        tempVcfOutputDir = Files.createTempDirectory("contigs_variants_dir");
    }

    @AfterAll
    public static void deleteTempFile() {
        tempVcfInputFileToTestFailingJobs.delete();
    }

    @BeforeEach
    public void setUp() throws Exception {
        if (!originalInputParametersCaptured) {
            originalVcfInputFilePath = inputParameters.getVcf();
            originalVcfContent = getOriginalVcfContent(originalVcfInputFilePath);
            writeToTempVCFFile(originalVcfContent, tempVcfInputFileToTestFailingJobs);
            originalInputParametersCaptured = true;
        }
        jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository);
        runner.setJobName(SUBSNP_ACCESSION_JOB);
        deleteTemporaryContigAndVariantFiles(inputParameters, tempVcfOutputDir);
        useOriginalVcfFile(inputParameters, originalVcfInputFilePath, vcfReader);

        mockServer = MockRestServiceServer.createServer(restTemplate);
        mockServer.expect(ExpectedCount.manyTimes(), requestTo(new URI(countServiceParameters.getUrl() + URL_PATH_SAVE_COUNT)))
                .andExpect(method(HttpMethod.POST))
                .andRespond(withStatus(HttpStatus.OK));

        mongoTemplate.dropCollection(SubmittedVariantEntity.class);

        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS contiguous_id_blocks");
        }
        ResourceDatabasePopulator populator = new ResourceDatabasePopulator();
        populator.addScript(new ClassPathResource("test-data/contiguous_id_blocks_schema.sql"));
        populator.execute(dataSource);
    }

    @AfterEach
    public void tearDown() {
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    public void runJobWithNoErrors() throws Exception {
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }

    @Test
    public void runJobWithNoName() throws Exception {
        runner.setJobName(null);
        runner.run();

        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());
    }
}