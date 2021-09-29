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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.clustering.batch.listeners.ClusteringCounts;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitCandidatesReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.RSMergeAndSplitWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.CLUSTERED_CLUSTERING_WRITER;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.RS_SPLIT_WRITER;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:merge-split-test.properties")
@ContextConfiguration(classes = {RSMergeAndSplitCandidatesReaderConfiguration.class, RSMergeAndSplitWriterConfiguration.class,
        MongoTestConfiguration.class, BatchTestConfiguration.class})
public class RSSplitWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000000001.1";

    @Rule
    public final ExpectedException thrown = ExpectedException.none();

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(RS_SPLIT_WRITER)
    private ItemWriter<SubmittedVariantOperationEntity> rsSplitWriter;

    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriter;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private SubmittedVariantEntity ss1, ss2, ss3, ss4, ss5, ss6;

    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Autowired
    private ClusteringCounts clusteringCounts;

    @Before
    public void setUp() {
        cleanupDB();
    }

    @After
    public void tearDown() {
        cleanupDB();
    }

    private void cleanupDB() {
        mongoClient.dropDatabase(TEST_DB);
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate) {

        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession, ASSEMBLY, 60711,
                                          "PRJ1", "chr1", start, reference, alternate, rsAccession, false, false, false,
                                          false, 1);
    }

    @Test
    public void testSplitsWithUnequalNumberOfDistinctHashes() throws Exception {
        Long sameRSAccessionToBeUsedForDifferentLoci = 1L;
        // Create SS with the same RS with ss2 and ss3 sharing the same locus (chr + start + type)
        ss1 = createSS(1L, sameRSAccessionToBeUsedForDifferentLoci, 100L, "C", "T");
        ss2 = createSS(2L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "T");
        ss3 = createSS(3L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "G");
        ss4 = createSS(4L, sameRSAccessionToBeUsedForDifferentLoci, 102L, "A", "C");

        // Create multiple entries in clustered variant collection - one for each distinct loci but with the same accession
        List<ClusteredVariantEntity> multipleEntriesWithSameRS = Stream.of(ss1, ss2, ss4)
                                                                       .map(clusteringWriter::toClusteredVariantEntity)
                                                                       .collect(Collectors.toList());

        mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3), DbsnpSubmittedVariantEntity.class);
        mongoTemplate.insert(Collections.singletonList(ss4), SubmittedVariantEntity.class);
        mongoTemplate.insert(multipleEntriesWithSameRS, DbsnpClusteredVariantEntity.class);
        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                            ss1.getAccession(), "Hash mismatch with " + ss1.getClusteredVariantAccession(),
                            Stream.of(ss1, ss2, ss3, ss4).map(SubmittedVariantInactiveEntity::new)
                                  .collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);
        rsSplitWriter.write(Collections.singletonList(splitOperation));

        // Per splitting policy, ensure that SS2 and SS3 get to retain the old RS
        // because the RS hash for start position 101 is supported in these 2 variants
        List<AccessionWrapper<ISubmittedVariant, String, Long>> ssAssociatedWithRS1 =
        submittedVariantAccessioningService.getByClusteredVariantAccessionIn(
                Collections.singletonList(sameRSAccessionToBeUsedForDifferentLoci));
        assertEquals(2, ssAssociatedWithRS1.size());
        assertTrue(ssAssociatedWithRS1.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet())
                                      .containsAll(Arrays.asList(ss2.getAccession(), ss3.getAccession())));

        /* Check clustering counts */
        // 2 new RS IDs created for hashes with start positions 100 and 102
        assertEquals(2, clusteringCounts.getClusteredVariantsCreated());
        // 2 RS split events for hashes with start positions 100 and 102
        assertEquals(2, clusteringCounts.getClusteredVariantsRSSplit());
        // 2 new RS IDs associated with ss1 and ss4
        assertEquals(2, clusteringCounts.getSubmittedVariantsUpdateOperationWritten());
        assertEquals(2, clusteringCounts.getSubmittedVariantsUpdatedRs());
    }

    @Test
    public void testSplitsWithEqualNumberOfDistinctHashes() throws Exception {
        Long sameRSAccessionToBeUsedForDifferentLoci = 1L;
        // Each RS locus (chr + start + type) is supported by two SS
        ss1 = createSS(1L, sameRSAccessionToBeUsedForDifferentLoci, 100L, "C", "T");
        ss2 = createSS(2L, sameRSAccessionToBeUsedForDifferentLoci, 100L, "C", "A");
        ss3 = createSS(3L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "T");
        ss4 = createSS(4L, sameRSAccessionToBeUsedForDifferentLoci, 101L, "A", "G");
        ss5 = createSS(5L, sameRSAccessionToBeUsedForDifferentLoci, 102L, "G", "T");
        ss6 = createSS(6L, sameRSAccessionToBeUsedForDifferentLoci, 102L, "G", "C");

        // Create multiple entries in clustered variant collection - one for each distinct loci but with the same accession
        List<ClusteredVariantEntity> multipleEntriesWithSameRS = Stream.of(ss1, ss3, ss5)
                                                                       .map(clusteringWriter::toClusteredVariantEntity)
                                                                       .collect(Collectors.toList());

        mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3), DbsnpSubmittedVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(ss4, ss5, ss6), SubmittedVariantEntity.class);
        mongoTemplate.insert(multipleEntriesWithSameRS, DbsnpClusteredVariantEntity.class);
        SubmittedVariantOperationEntity splitOperation = new SubmittedVariantOperationEntity();
        splitOperation.fill(RSMergeAndSplitCandidatesReaderConfiguration.SPLIT_CANDIDATES_EVENT_TYPE,
                            ss1.getAccession(), "Hash mismatch with " + ss1.getClusteredVariantAccession(),
                            Stream.of(ss1, ss2, ss3, ss4, ss5, ss6).map(SubmittedVariantInactiveEntity::new)
                                  .collect(Collectors.toList())
        );
        mongoTemplate.insert(Collections.singletonList(splitOperation), SubmittedVariantOperationEntity.class);
        rsSplitWriter.write(Collections.singletonList(splitOperation));

        // Per splitting policy, ensure that SS1 and SS2 get to retain the old RS
        // Even though locus with start 100, 101 and 102 have equal number of supporting variants
        // the group SS1 and SS2 with locus 100 has the oldest SS ID and gets to keep the old RS ID
        List<AccessionWrapper<ISubmittedVariant, String, Long>> ssAssociatedWithRS1 =
                submittedVariantAccessioningService.getByClusteredVariantAccessionIn(
                        Collections.singletonList(sameRSAccessionToBeUsedForDifferentLoci));
        assertEquals(2, ssAssociatedWithRS1.size());
        assertTrue(ssAssociatedWithRS1.stream().map(AccessionWrapper::getAccession).collect(Collectors.toSet())
                                      .containsAll(Arrays.asList(ss1.getAccession(), ss2.getAccession())));

        /* Check clustering counts */
        // 2 entries created for hashes with start positions 100 and 102
        assertEquals(2, clusteringCounts.getClusteredVariantsCreated());
        // 2 RS split events for hashes with start positions 100 and 102
        assertEquals(2, clusteringCounts.getClusteredVariantsRSSplit());
        // 2 new RS IDs associated with SS groups: ss3,ss4 and ss5,ss6
        assertEquals(4, clusteringCounts.getSubmittedVariantsUpdateOperationWritten());
        assertEquals(4, clusteringCounts.getSubmittedVariantsUpdatedRs());
    }
}