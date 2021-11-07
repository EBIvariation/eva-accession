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
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringMongoReader;
import uk.ac.ebi.eva.accession.clustering.batch.io.ClusteringWriter;
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
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.springframework.data.mongodb.core.query.Criteria.where;
import static uk.ac.ebi.eva.accession.clustering.batch.io.clustering_writer.ClusteringAssertions.assertClusteringCounts;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.BACK_PROPAGATED_RS_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.NON_CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_MERGE_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_CANDIDATES_READER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;

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
@TestPropertySource("classpath:clustering-writer-test.properties")
public class ReuseAccessionClusteringWriterTest {

    private static final String TEST_DB = "test-db";

    private static String ASM_1;

    private static String ASM_2;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    private MetricCompute metricCompute;

    @Autowired
    private ClusteredVariantAccessioningService clusteredVariantAccessioningService;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    // Current clustering sequence is:
    // generate merge split candidates from clustered variants -> perform merge
    // -> perform split -> cluster new variants
    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriterPreMergeAndSplit;

    @Autowired
    @Qualifier(NON_CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriterPostMergeAndSplit;

    @Autowired
    @Qualifier(RS_MERGE_CANDIDATES_READER)
    private ItemReader<SubmittedVariantOperationEntity> rsMergeCandidatesReader;

    @Autowired
    @Qualifier(RS_SPLIT_CANDIDATES_READER)
    private ItemReader<SubmittedVariantOperationEntity> rsSplitCandidatesReader;

    @Autowired
    @Qualifier(RS_MERGE_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsMergeWriter;

    @Autowired
    @Qualifier(RS_SPLIT_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsSplitWriter;

    @Autowired
    @Qualifier(CLEAR_RS_MERGE_AND_SPLIT_CANDIDATES)
    private ItemWriter clearRSMergeAndSplitCandidates;

    @Autowired
    @Qualifier(BACK_PROPAGATED_RS_WRITER)
    private ItemWriter<SubmittedVariantEntity> backPropagatedRSWriter;

    private Function<ISubmittedVariant, String> hashingFunction;

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() {
        ASM_1 = inputParameters.getRemappedFrom();
        ASM_2 = inputParameters.getAssemblyAccession();
        mongoTemplate.getDb().drop();
        hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
        metricCompute.clearCount();
    }

    @Test
    @DirtiesContext
    public void reuse_clustered_accession_if_provided() throws Exception {
        Long existingRs = 3000000000L;

        ClusteredVariantEntity cve1 = createClusteredVariantEntity(ASM_2, existingRs);
        mongoTemplate.insert(cve1);
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // keep the RS that a submitted variant already has (RS=existingRs)
        SubmittedVariantEntity sveClustered = createSubmittedVariantEntity(ASM_2, existingRs, 5000000000L);
        this.clusterVariants(Collections.singletonList(sveClustered));
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // assign an existing RS to a different submitted variant (that has rs=null)
        createSubmittedVariantEntity(ASM_1, null, 5100000000L);
        SubmittedVariantEntity sveNonClustered = createSubmittedVariantEntity(ASM_2, null, 5100000000L);
        this.clusterVariants(Collections.singletonList(sveNonClustered));
        // Two RS - one reused for clustering the non-clustered variant sveNonClustered in the remapped assembly
        // and another for back-propagating that new RS to the original assembly
        assertEquals(2, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // for the same submitted variant without an assigned RS (rs=null), getOrCreate should not create another RS
        this.clusterVariants(Collections.singletonList(sveNonClustered));
        assertEquals(2, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));

        // Two RS - one reused for clustering the non-clustered variant sveNonClustered in the remapped assembly
        // and another for back-propagating that new RS to the original assembly
        assertClusteringCounts(clusteringCounts, 1, 0, 0, 0, 2, 0, 2);
        assertClusteringCounts(metricCompute, 0, 0, 0, 0, 2, 0, 2);
        assertClusteringCounts(metricCompute, 1, 0, 0, 0, 2, 0, 2);
    }

    private SubmittedVariantEntity createSubmittedVariantEntity(String assembly, Long rs, Long ss) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, 1000, "project", "1", 100L, "T", "A", rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(ss, hash1, submittedClustered, 1);
        submittedVariantEntity.setRemappedFrom(ASM_1);
        mongoTemplate.save(submittedVariantEntity, mongoTemplate.getCollectionName(SubmittedVariantEntity.class));
        return submittedVariantEntity;
    }

    private DbsnpSubmittedVariantEntity createDbsnpSubmittedVariantEntity(String assembly, Long rs, Long ss,
                                                                          String remappedFromAssembly) {
        SubmittedVariant submittedClustered = new SubmittedVariant(assembly, 1000, "project", "1", 100L, "T", "A", rs);
        String hash1 = hashingFunction.apply(submittedClustered);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity = new DbsnpSubmittedVariantEntity(ss, hash1,
                                                                                                  submittedClustered,
                                                                                                  1);
        dbsnpSubmittedVariantEntity.setRemappedFrom(remappedFromAssembly);
        mongoTemplate.save(dbsnpSubmittedVariantEntity,
                           mongoTemplate.getCollectionName(DbsnpSubmittedVariantEntity.class));
        return dbsnpSubmittedVariantEntity;
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

        mongoTemplate.insert(createDbsnpClusteredVariantEntity(ASM_2, rs1));

        createSubmittedVariantEntity(ASM_1, null, ss);
        SubmittedVariantEntity sveNonClustered = createSubmittedVariantEntity(ASM_2, null, ss);

        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(2, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        // when
        this.clusterVariants(Collections.singletonList(sveNonClustered));

        // then
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        // Two RS records: one clustering SS in the remapped assembly with rs1 (already existing) and another
        // for back-propagating rs1 to the SS in the older assembly (newly created)
        assertEquals(2, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(2, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        // Two operations: one for clustering SS in the remapped assembly with rs1 and another
        // for back-propagating rs1 to the SS in the older assembly
        assertEquals(2, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        SubmittedVariantOperationEntity operation = mongoTemplate.findOne(new Query(where("accession").is(ss)),
                SubmittedVariantOperationEntity.class);
        assertNotNull(operation.getCreatedDate());

        SubmittedVariantEntity afterClustering = mongoTemplate.findOne(new Query(), SubmittedVariantEntity.class);
        assertEquals(rs1, afterClustering.getClusteredVariantAccession());
        SubmittedVariantOperationEntity afterClusteringOperation = mongoTemplate.findOne(
                new Query(), SubmittedVariantOperationEntity.class);
        assertEquals(sveNonClustered.getAccession(), afterClusteringOperation.getAccession());

        // One newly created RS due to back-propagation of rs1 to the SS in the older assembly
        assertClusteringCounts(clusteringCounts, 1, 0, 0, 0, 2, 0, 2);
        assertClusteringCounts(metricCompute, 0, 0, 0, 0, 2, 0, 2);
        assertClusteringCounts(metricCompute, 1, 0, 0, 0, 2, 0, 2);
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

        mongoTemplate.insert(createClusteredVariantEntity(ASM_2, rs1));

        SubmittedVariantEntity sveNonClusteredOriginalAssembly = createDbsnpSubmittedVariantEntity(ASM_1, null, ss,
                                                                                                   null);
        SubmittedVariantEntity sveNonClusteredRemappedAssembly = createDbsnpSubmittedVariantEntity(ASM_2, null, ss,
                                                                                                   ASM_1);

        assertEquals(2, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        assertEquals(1, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        // when
        this.clusterVariants(Collections.singletonList(sveNonClusteredRemappedAssembly));

        // then
        assertEquals(2, mongoTemplate.count(new Query(), DbsnpSubmittedVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), DbsnpClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantEntity.class));
        // Two records: Newly generated RS in the new assembly and the same RS back-propagated to the older assembly
        assertEquals(2, mongoTemplate.count(new Query(), ClusteredVariantEntity.class));
        assertEquals(0, mongoTemplate.count(new Query(), SubmittedVariantOperationEntity.class));
        // Two operations: one for clustering SS in the remapped assembly with rs1 and another
        // for back-propagating rs1 to the SS in the older assembly
        assertEquals(2, mongoTemplate.count(new Query(), DbsnpSubmittedVariantOperationEntity.class));

        DbsnpSubmittedVariantOperationEntity operation = mongoTemplate.findOne(new Query(where("accession").is(ss)),
                DbsnpSubmittedVariantOperationEntity.class);
        assertNotNull(operation.getCreatedDate());

        SubmittedVariantEntity afterClustering = mongoTemplate.findOne(new Query(), DbsnpSubmittedVariantEntity.class);
        assertEquals(rs1, afterClustering.getClusteredVariantAccession());
        DbsnpSubmittedVariantOperationEntity afterClusteringOperation = mongoTemplate.findOne(
                new Query(), DbsnpSubmittedVariantOperationEntity.class);
        assertEquals(sveNonClusteredRemappedAssembly.getAccession(), afterClusteringOperation.getAccession());

        // One RS expected to be created due to back-propagated RS1
        assertClusteringCounts(clusteringCounts, 1, 0, 0, 0, 2, 0, 2);
        assertClusteringCounts(metricCompute, 0, 0, 0, 0, 2, 0, 2);
        assertClusteringCounts(metricCompute, 1, 0, 0, 0, 2, 0, 2);
    }

    private void clusterVariants(List<SubmittedVariantEntity> submittedVariantEntities)
            throws Exception {
        clusteringWriterPreMergeAndSplit.write(submittedVariantEntities);
        List<SubmittedVariantOperationEntity> mergeCandidates = new ArrayList<>();
        List<SubmittedVariantOperationEntity> splitCandidates = new ArrayList<>();
        SubmittedVariantOperationEntity tempSVO;
        while((tempSVO = rsMergeCandidatesReader.read()) != null) {
            mergeCandidates.add(tempSVO);
        }
        while((tempSVO = rsSplitCandidatesReader.read()) != null) {
            splitCandidates.add(tempSVO);
        }
        rsMergeWriter.write(mergeCandidates);
        rsSplitWriter.write(splitCandidates);

        ClusteringMongoReader unclusteredVariantsReader = new ClusteringMongoReader(this.mongoTemplate, ASM_2, 100,
                                                                                    false);
        unclusteredVariantsReader.initializeReader();
        List<SubmittedVariantEntity> unclusteredVariants = new ArrayList<>();
        SubmittedVariantEntity tempSV;
        while((tempSV = unclusteredVariantsReader.read()) != null) {
            unclusteredVariants.add(tempSV);
        }
        unclusteredVariantsReader.close();
        // Cluster non-clustered variants
        clusteringWriterPostMergeAndSplit.write(unclusteredVariants);
        // Spring has a mandatory requirement of even small functionality being writers.
        // To satisfy that, we pass in a dummy object to invoke the writer
        // which basically clears the merge and split operations after they were processed above
        clearRSMergeAndSplitCandidates.write(Collections.singletonList(new Object()));
        ClusteringMongoReader originalAssemblyUnclusteredVariantsReader =
                new ClusteringMongoReader(this.mongoTemplate, ASM_1, 100, false);
        originalAssemblyUnclusteredVariantsReader.initializeReader();
        while((tempSV = originalAssemblyUnclusteredVariantsReader.read()) != null) {
            backPropagatedRSWriter.write(Collections.singletonList(tempSV));
        }
    }
}
