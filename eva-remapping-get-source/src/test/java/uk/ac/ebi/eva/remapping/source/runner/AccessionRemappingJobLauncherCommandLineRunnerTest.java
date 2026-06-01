/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.source.runner;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.remapping.source.parameters.InputParameters;
import uk.ac.ebi.eva.remapping.source.parameters.ReportPathResolver;
import uk.ac.ebi.eva.remapping.source.test.configuration.BatchJobRepositoryTestConfiguration;
import uk.ac.ebi.eva.remapping.source.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.remapping.source.test.configuration.MongoTestConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        BatchJobRepositoryTestConfiguration.class})
@TestPropertySource("classpath:application.properties")
public class AccessionRemappingJobLauncherCommandLineRunnerTest extends MongoTestContainerHelper {
    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private AccessionRemappingJobLauncherCommandLineRunner runner;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        deleteOutputFiles();

        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load("/test-data/dbsnpSubmittedVariantEntity.json");
        mongoTestDataLoader.load("/test-data/submittedVariantEntity.json");
    }

    @AfterEach
    public void tearDown() {
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
    public void contextLoads() throws JobExecutionException {
        runner.run();
        assertEquals(AccessionRemappingJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS, runner.getExitCode());
    }
}