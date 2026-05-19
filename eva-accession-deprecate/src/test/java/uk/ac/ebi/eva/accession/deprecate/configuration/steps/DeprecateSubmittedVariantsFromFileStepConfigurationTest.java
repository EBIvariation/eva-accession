/*
 * Copyright 2024 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.configuration.steps;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup;
import uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.BatchJobRepositoryTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.accession.deprecate.test.configuration.BatchTestConfiguration.JOB_LAUNCHER_FROM_FILE;

@ExtendWith(SpringExtension.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class,
        BatchJobRepositoryTestConfiguration.class})
@TestPropertySource("classpath:deprecate-submitted-variants-from-file-test.properties")
public class DeprecateSubmittedVariantsFromFileStepConfigurationTest extends MongoTestContainerHelper {
    @Autowired
    @Qualifier(JOB_LAUNCHER_FROM_FILE)
    private JobLauncherTestUtils jobLauncherTestUtilsFromFile;

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        MongoTestDatabaseSetup.populateTestDBForFile(this.mongoTemplate);
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void variantsDeprecated() {
        JobExecution jobExecution = jobLauncherTestUtilsFromFile.launchStep(BeanNames.DEPRECATE_SUBMITTED_VARIANTS_FROM_FILE_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
        MongoTestDatabaseSetup.assertPostDeprecationDatabaseState(this.mongoTemplate);
    }
}