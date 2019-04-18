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
package uk.ac.ebi.eva.accession.deprecate.configuration;

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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.deprecate.test.rule.FixSpringMongoDbRule;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.deprecate.configuration.BeanNames.DEPRECATE_CLUSTERED_VARIANTS_STEP;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class, MongoTestConfiguration.class})
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json",
        "/test-data/dbsnpClusteredVariantEntityDeclustered.json"})
@TestPropertySource("classpath:application_species_subset.properties")
public class DeprecateSubsetClusteredVariantsStepConfigurationTest {

    private static final String TEST_DB = "test-db";

    private static final String DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED = "dbsnpClusteredVariantEntityDeclustered";

    private static final long EXPECTED_VARIANTS_TO_BE_NOT_FULLY_DECLUSTERED = 7;

    // from those listed in assemblyAccession (in the properties file), only GCA_000000003.1 is not present in
    // dbsnpSubmittedVariantEntity
    private static final long EXPECTED_OPERATIONS = 1;

    @Autowired
    private JobLauncherTestUtils jobLauncherTestUtils;

    @Autowired
    private MongoTemplate mongoTemplate;

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
    public void stepCompletion() {
        assertStepExecutesAndCompletes();
    }

    private void assertStepExecutesAndCompletes() {
        JobExecution jobExecution = jobLauncherTestUtils.launchStep(DEPRECATE_CLUSTERED_VARIANTS_STEP);
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());
    }

    @Test
    public void variantsDeprecated() {
        assertStepExecutesAndCompletes();
        assertEquals(EXPECTED_VARIANTS_TO_BE_NOT_FULLY_DECLUSTERED,
                     mongoTemplate.getCollection(DBSNP_CLUSTERED_VARIANT_ENTITY_DECLUSTERED).count());
        assertOperations();
    }

    private void assertOperations() {
        List<DbsnpClusteredVariantOperationEntity> operations = mongoTemplate
                .find(new Query(), DbsnpClusteredVariantOperationEntity.class);
        assertEquals(EXPECTED_OPERATIONS, operations.size());
        assertEquals(EXPECTED_OPERATIONS,
                     operations.stream().filter(o -> o.getEventType().equals(EventType.DEPRECATED)).count());
    }
}