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
package uk.ac.ebi.eva.accession.clustering.batch.io.clustering_writer;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.repositories.ContiguousIdBlockRepository;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Collections;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static uk.ac.ebi.eva.accession.clustering.batch.io.clustering_writer.ClusteringAssertions.assertClusteringCounts;

/**
 * This class handles some scenarios of ClusteringWriter where an existing RS is reused.
 *
 * The scenarios tested here are those about either reusing the clustered variant accessions (RSs) that a submitted
 * variant provides, or reusing an RS that exists in the database because it's equivalent (shares identifying fields).
 * Other test classes in this folder take care of other scenarios.
 */
@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, BatchTestConfiguration.class})
@TestPropertySource("classpath:clustering-pipeline-test.properties")
public class ReuseAccessionClusteringWriterTest {

    private static final String TEST_DB = "test-db";

    public static final long EVA_CLUSTERED_VARIANT_RANGE_START = 3000000000L;

    public static final long EVA_SUBMITTED_VARIANT_RANGE_START = 5000000000L;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ClusteringCounts clusteringCounts;

    @Autowired
    private ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    @Autowired
    private ContiguousIdBlockRepository contiguousIdBlockRepository;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    private ClusteringWriter clusteringWriter;

    private Function<ISubmittedVariant, String> hashingFunction;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() {
        mongoTemplate.getDb().drop();
        clusteringWriter = new ClusteringWriter(mongoTemplate, clusteredVariantAccessioningService, EVA_SUBMITTED_VARIANT_RANGE_START,
                EVA_CLUSTERED_VARIANT_RANGE_START, clusteringCounts);
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    public void reuse_clustered_accession_if_provided() throws AccessionCouldNotBeGeneratedException,
            AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        Long existingRs = 3000000000L;
        String asm1 = "asm1";
        String asm2 = "asm2";

        ClusteredVariantEntity cve1 = createClusteredVariantEntity(asm1, existingRs);
        mongoTemplate.insert(cve1);
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // keep the RS that a submitted variant already has (RS=existingRs)
        SubmittedVariantEntity sveClustered = createSubmittedVariantEntity(asm1, existingRs, 5000000000L);
        clusteringWriter.write(Collections.singletonList(sveClustered));
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // assign an existing RS to a different submitted variant (that has rs=null)
        SubmittedVariantEntity sveNonClustered = createSubmittedVariantEntity(asm2, null, 5100000000L);
        clusteringWriter.write(Collections.singletonList(sveNonClustered));
        assertEquals(2, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // for the same submitted variant without an assigned RS (rs=null), getOrCreate should not create another RS
        clusteringWriter.write(Collections.singletonList(sveNonClustered));
        assertEquals(2, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        assertClusteringCounts(clusteringCounts, 1, 0, 0, 0, 2, 0, 2);
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(String assembly, Long rs, Long ss) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, 1000, "project", "1", 100L, "T", "A", rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        return new SubmittedVariantEntity(ss, hash1, submittedClustered, 1);
    }

    private DbsnpSubmittedVariantEntity createDbsnpSubmittedVariantEntity(String assembly, Long rs, Long ss) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, 1000, "project", "1", 100L, "T", "A", rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        return new DbsnpSubmittedVariantEntity(ss, hash1, submittedClustered, 1);
    }

    private ClusteredVariantEntity createClusteredVariantEntity(String assembly, Long rs) {
        ClusteredVariant cv = new ClusteredVariant(assembly, 1000, "1", 100L, VariantType.SNV, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new ClusteredVariantEntity(rs, cvHash, cv, 1);
    }

    private DbsnpClusteredVariantEntity createDbsnpClusteredVariantEntity(String assembly, Long rs) {
        ClusteredVariant cv = new ClusteredVariant(assembly, 1000, "1", 100L, VariantType.SNV, false, null);
        String cvHash = clusteredHashingFunction.apply(cv);
        return new DbsnpClusteredVariantEntity(rs, cvHash, cv, 1);
    }

    @Test
    @DirtiesContext
    public void reuse_dbsnp_clustered_accession_when_clustering_an_eva_submitted_variant() throws Exception {
        // given
        Long rs1 = 30L;
        long ss = 5100000000L;

        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        mongoTemplate.insert(createDbsnpClusteredVariantEntity("asm1", rs1));

        SubmittedVariantEntity sveNonClustered = createSubmittedVariantEntity("asm1", null, ss);
        mongoTemplate.insert(sveNonClustered);

        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        // when
        clusteringWriter.write(Collections.singletonList(sveNonClustered));

        // then
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        SubmittedVariantOperationEntity operation = mongoTemplate.findOne(new Query(where("accession").is(ss)),
                SubmittedVariantOperationEntity.class);
        assertNotNull(operation.getCreatedDate());

        SubmittedVariantEntity afterClustering = mongoTemplate.findOne(new Query(), SubmittedVariantEntity.class);
        assertEquals(rs1, afterClustering.getClusteredVariantAccession());
        SubmittedVariantOperationEntity afterClusteringOperation = mongoTemplate.findOne(
                new Query(), SubmittedVariantOperationEntity.class);
        assertEquals(sveNonClustered.getAccession(), afterClusteringOperation.getAccession());

        assertClusteringCounts(clusteringCounts, 0, 0, 0, 0, 1, 0, 1);
    }

    @Test
    @DirtiesContext
    public void reuse_eva_clustered_accession_when_clustering_a_dbsnp_submitted_variant() throws Exception {
        // given
        Long rs1 = 3000000000L;
        long ss = 51L;

        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        mongoTemplate.insert(createClusteredVariantEntity("asm1", rs1));

        SubmittedVariantEntity sveNonClustered = createDbsnpSubmittedVariantEntity("asm1", null, ss);
        mongoTemplate.insert(sveNonClustered);

        assertEquals(1, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        // when
        clusteringWriter.write(Collections.singletonList(sveNonClustered));

        // then
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        DbsnpSubmittedVariantOperationEntity operation = mongoTemplate.findOne(new Query(where("accession").is(ss)),
                DbsnpSubmittedVariantOperationEntity.class);
        assertNotNull(operation.getCreatedDate());

        SubmittedVariantEntity afterClustering = mongoTemplate.findOne(new Query(), DbsnpSubmittedVariantEntity.class);
        assertEquals(rs1, afterClustering.getClusteredVariantAccession());
        DbsnpSubmittedVariantOperationEntity afterClusteringOperation = mongoTemplate.findOne(
                new Query(), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(sveNonClustered.getAccession(), afterClusteringOperation.getAccession());

        assertClusteringCounts(clusteringCounts, 0, 0, 0, 0, 1, 0, 1);
    }
}
