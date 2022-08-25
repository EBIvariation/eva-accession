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
 *
 */
package uk.ac.ebi.eva.remapping.ingest.configuration.batch.steps;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.remapping.ingest.configuration.BeanNames;
import uk.ac.ebi.eva.remapping.ingest.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.remapping.ingest.test.rule.FixSpringMongoDbRule;

import java.util.List;
import java.util.function.Function;

import static junit.framework.TestCase.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestPropertySource("classpath:ingest-remapped-variants.properties")
@UsingDataSet(locations = {"/test-data/submittedVariantEntity.json"})
public class IngestRemappedFromVcfStepConfigurationTest {

    private static final String TEST_DB = "test-ingest-remapping";

    private static final String SUBMITTED_VARIANT_COLLECTION = "submittedVariantEntity";

    public static final String REMAPPED_FROM = "GCA_000000001.1";

    private Function<ISubmittedVariant, String> hashingFunction;

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

    @Before
    public void setUp() throws Exception {
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    public void runStep() {
        //Documents in the database before the ingestion
        assertEquals(7, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());

        JobExecution jobExecution = jobLauncherTestUtils.launchStep(BeanNames.INGEST_REMAPPED_VARIANTS_FROM_VCF_STEP);
        Assert.assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        //Documents in the database after the ingestion
        assertEquals(11, mongoTemplate.getCollection(SUBMITTED_VARIANT_COLLECTION).countDocuments());

        Query remappedVariantsQuery = new Query(Criteria.where("remappedFrom").is(REMAPPED_FROM));
        List<SubmittedVariantEntity> remappedVariants = mongoTemplate.find(remappedVariantsQuery,
                                                                           SubmittedVariantEntity.class);

        assertEquals(6, remappedVariants.size());

        //Variant ss5000000000: Remapped only once
        assertEquals(2, getVariantCountBySsId(5000000000L));

        //Variant ss5000000004: Remapped only once not clustered
        assertEquals(2, getVariantCountBySsId(5000000004L));
        List<SubmittedVariantEntity> variants = getVariantsBySsId(5000000004L);
        assertEquals(0, variants.stream().filter(x -> x.getClusteredVariantAccession() != null).count());

        //Variant ss5000000001: Remapped twice to the same location
        //Skip the duplicate variant as they have the same hash
        assertEquals(2, getVariantCountBySsId(5000000001L));

        //Variant ss5000000002: Remapped twice to a different location
        //Skip the duplicate variant as they have the same accession
        assertEquals(2, getVariantCountBySsId(5000000002L));

        //Variant ss5000000002: Remapped only once, belongs to a different project and have a different taxonomy
        assertEquals(2, getVariantCountBySsId(5000000003L));

        //Variant ss5000000005: Ingested with backpropagated RS
        List<SubmittedVariantEntity> bpVariants = getVariantsBySsId(5000000005L);
        assertEquals(1, bpVariants.stream().filter(x -> x.getBackPropagatedVariantAccession() == 3000000004L).count());
    }

    private long getVariantCountBySsId(long ssId) {
        Query query = new Query(Criteria.where("accession").is(ssId));
        return mongoTemplate.count(query, SubmittedVariantEntity.class);
    }

    private List<SubmittedVariantEntity> getVariantsBySsId(long ssId) {
        Query query = new Query(Criteria.where("accession").is(ssId));
        return mongoTemplate.find(query, SubmittedVariantEntity.class);
    }

}