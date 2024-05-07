package uk.ac.ebi.eva.accession.pipeline.runner;

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

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.test.context.SpringBatchTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.parameters.InputParameters;
import uk.ac.ebi.eva.accession.pipeline.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.pipeline.configuration.BeanNames.CREATE_SUBSNP_ACCESSION_JOB;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.deleteTemporaryContigAndVariantFiles;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.getOriginalVcfContent;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.injectErrorIntoTempVcf;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.useOriginalVcfFile;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.useTempVcfFile;
import static uk.ac.ebi.eva.accession.pipeline.runner.RunnerUtil.writeToTempVCFFile;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
@SpringBatchTest
public class JobFailureBlocksReleasedTest {
    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private EvaAccessionJobLauncherCommandLineRunner runner;

    @Autowired
    private VcfReader vcfReader;

    @Autowired
    private MongoTemplate mongoTemplate;

    private static File tempVcfInputFileToTestFailingJobs;

    private static Path tempVcfOutputDir;

    private static String originalVcfInputFilePath;

    private static String originalVcfContent;

    @Autowired
    private ContiguousIdBlockRepository blockRepository;

    @BeforeClass
    public static void initializeTempFile() throws Exception {
        tempVcfInputFileToTestFailingJobs = File.createTempFile("resumeFailingJob", ".vcf.gz");
        tempVcfOutputDir = Files.createTempDirectory("contigs_variants_dir");
    }

    @AfterClass
    public static void deleteTempFile() {
        tempVcfInputFileToTestFailingJobs.delete();
    }

    @Before
    public void setUp() throws Exception {
        originalVcfInputFilePath = inputParameters.getVcf();
        originalVcfContent = getOriginalVcfContent(originalVcfInputFilePath);
        writeToTempVCFFile(originalVcfContent, tempVcfInputFileToTestFailingJobs);

        runner.setJobNames(CREATE_SUBSNP_ACCESSION_JOB);
        deleteTemporaryContigAndVariantFiles(inputParameters, tempVcfOutputDir);
        useOriginalVcfFile(inputParameters, originalVcfInputFilePath, vcfReader);

        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
    }

    @Test
    @DirtiesContext
    public void testContiguousBlocksAreReleasedInCaseOfJobFailures() throws Exception {
        useTempVcfFile(inputParameters, tempVcfInputFileToTestFailingJobs, vcfReader);
        String modifiedVcfContent = originalVcfContent.replace("76852", "76852jibberish");
        injectErrorIntoTempVcf(modifiedVcfContent, tempVcfInputFileToTestFailingJobs);
        // run the job and check the job failed
        runner.run();
        assertEquals(EvaAccessionJobLauncherCommandLineRunner.EXIT_WITH_ERRORS, runner.getExitCode());

        // Check reserved blocks are being released even in case of job failures
        blockRepository.findAll().forEach(block -> {
            assertTrue(block.isNotReserved());
        });
    }

}