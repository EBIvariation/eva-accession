/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.core.batch.io;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.Arrays;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:ss-deprecation-test.properties")
@EnableAutoConfiguration
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class,
        SubmittedVariantAccessioningConfiguration.class, ClusteredVariantAccessioningConfiguration.class})
public class ClusteredVariantDeprecationWriterTest {

    private static final String TEST_DB = "sve-deprecation-test";

    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final int TAXONOMY = 60711;

    @Autowired
    private Long accessioningMonotonicInitRs;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    private SubmittedVariantEntity ss1, ss2, ss3, ss4;
    private ClusteredVariantEntity rs1, rs2;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private void cleanup() {
        mongoClient.dropDatabase(TEST_DB);
    }

    @Before
    public void setUp() {
        cleanup();
    }

    @After
    public void tearDown() {
        cleanup();
    }

    @Test
    public void testDeprecateRS() {
        // rs1 -> ss1,ss2
        // rs2 -> ss3,ss4
        ss1 = createSS(1L, 1L, 100L, "C", "T");
        ss2 = createSS(2L, 1L, 100L, "C", "A");
        ss3 = createSS(5L, 5L, 102L, "T", "G");
        ss4 = createSS(6L, 5L, 102L, "T", "A");

        this.mongoTemplate.insert(Arrays.asList(ss1, ss2), DbsnpSubmittedVariantEntity.class);
        rs1 = this.createRS(ss1);
        this.mongoTemplate.save(rs1, this.mongoTemplate.getCollectionName(DbsnpClusteredVariantEntity.class));

        this.mongoTemplate.insert(Arrays.asList(ss3, ss4), SubmittedVariantEntity.class);
        rs2 = this.createRS(ss3);
        this.mongoTemplate.save(rs2, this.mongoTemplate.getCollectionName(ClusteredVariantEntity.class));

        ClusteredVariantDeprecationWriter cveDeprecationWriter =
                new ClusteredVariantDeprecationWriter(ASSEMBLY, this.mongoTemplate,
                                                      this.submittedVariantAccessioningService,
                                                      this.accessioningMonotonicInitRs,
                                                      "TEST", "Deprecation test");

        this.mongoTemplate.remove(ss1, this.mongoTemplate.getCollectionName(DbsnpSubmittedVariantEntity.class));
        this.mongoTemplate.remove(ss2, this.mongoTemplate.getCollectionName(DbsnpSubmittedVariantEntity.class));
        this.mongoTemplate.remove(ss3, this.mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        cveDeprecationWriter.write(Arrays.asList(rs1, rs2));
        assertPostDeprecationDatabaseState();

        // Ensure that the second run of deprecation does not do any harm
        cveDeprecationWriter.write(Arrays.asList(rs1, rs2));
        assertPostDeprecationDatabaseState();
    }

    private void assertPostDeprecationDatabaseState() {
        // Ensure that only the RS with accession 1 (rs1) is deprecated
        // because the RS with accession 5 (rs2) is still associated with ss4
        assertEquals(0, this.mongoTemplate.findAll(DbsnpClusteredVariantEntity.class).size());
        assertEquals(1, this.mongoTemplate.findAll(ClusteredVariantEntity.class).size());
        assertEquals(1, this.mongoTemplate.findAll(DbsnpClusteredVariantOperationEntity.class).size());
        assertEquals(0, this.mongoTemplate.findAll(ClusteredVariantOperationEntity.class).size());
        DbsnpClusteredVariantOperationEntity rs1DeprecationOp =
                this.mongoTemplate.findById("RS_DEPRECATED_TEST_" + rs1.getHashedMessage(),
                                            DbsnpClusteredVariantOperationEntity.class);
        assertNotNull(rs1DeprecationOp);
        assertEquals(rs1, rs1DeprecationOp.getInactiveObjects().get(0).toClusteredVariantEntity());
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate) {

        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession, ASSEMBLY, TAXONOMY,
                                          "PRJ1", "chr1", start, reference, alternate, rsAccession, false, false, false,
                                          false, 1);
    }

    private ClusteredVariantEntity createRS(SubmittedVariantEntity sve) {
        Function<IClusteredVariant, String> hashingFunction =  new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariant cv = new ClusteredVariant(sve.getReferenceSequenceAccession(), sve.getTaxonomyAccession(),
                                                   sve.getContig(),
                                                   sve.getStart(),
                                                   new Variant(sve.getContig(), sve.getStart(), sve.getStart(),
                                                               sve.getReferenceAllele(),
                                                               sve.getAlternateAllele()).getType(),
                                                   true, null);
        String hash = hashingFunction.apply(cv);
        return new ClusteredVariantEntity(sve.getClusteredVariantAccession(), hash, cv);
    }

}
