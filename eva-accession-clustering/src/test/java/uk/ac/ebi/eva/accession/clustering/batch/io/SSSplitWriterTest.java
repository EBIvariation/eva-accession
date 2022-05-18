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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.SSSplitWriterConfiguration;
import uk.ac.ebi.eva.accession.clustering.metric.ClusteringMetric;
import uk.ac.ebi.eva.accession.clustering.test.DatabaseState;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doThrow;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.SS_SPLIT_WRITER;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:merge-split-test.properties")
@ContextConfiguration(classes = {SSSplitWriterConfiguration.class, MongoTestConfiguration.class,
        BatchTestConfiguration.class})
public class SSSplitWriterTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000000001.1";

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    @Qualifier(SS_SPLIT_WRITER)
    private ItemWriter<SubmittedVariantEntity> ssSplitWriter;

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
    private MetricCompute<ClusteringMetric> metricCompute;

    @Before
    public void setUp() {
        cleanup();
    }

    @After
    public void tearDown() {
        cleanup();
    }

    private void cleanup() {
        mongoClient.dropDatabase(TEST_DB);
        metricCompute.clearCount();
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, Long start, String reference,
                                            String alternate, boolean supportedByEvidence, boolean allelesMatch) {
        Function<ISubmittedVariant, String> hashingFunction = new SubmittedVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        SubmittedVariant temp = new SubmittedVariant(ASSEMBLY, 60711, "PRJ1", "chr1", start, reference, alternate,
                                                     rsAccession);
        temp.setSupportedByEvidence(supportedByEvidence);
        temp.setAllelesMatch(allelesMatch);
        return new SubmittedVariantEntity(ssAccession, hashingFunction.apply(temp), temp, 1);
    }

    private void setupSplitScenario() {
        // Create SS with the different loci sharing same IDs
        ss1 = createSS(1L, 1L, 100L, "C", "T", true, true);
        ss2 = createSS(1L, 1L, 100L, "C", "G", false, true);
        ss3 = createSS(1L, null, 100L, "C", "A", true, true);
        ss4 = createSS(1L, 1L, 100L, "A", "C", true, true);

        ss5 = createSS(2L, 2L, 103L, "A", "C", false, true);
        ss6 = createSS(2L, 2L, 103L, "A", "T", false, false);

        this.mongoTemplate.insert(Arrays.asList(ss1, ss2, ss3, ss4, ss5, ss6), DbsnpSubmittedVariantEntity.class);
    }

    private void writeSplitWithoutCrashes() throws Exception {
        setupSplitScenario();
        ssSplitWriter.write(Arrays.asList(ss1, ss2, ss3, ss4, ss5, ss6));
    }

    @Test
    public void testSplitsWithoutCrashes() throws Exception {
        writeSplitWithoutCrashes();
        assertPostSplitStatus();
    }

    private void assertPostSplitStatus() {
        // see setupSplitScenario() for the database state setup for SS split
        // prioritize(ss4, prioritize(ss3, prioritize(ss1,ss2)))
        // prioritize(ss1,ss2) = ss1 due to ss1 being supported by evidence
        // prioritize(ss3, ss1) = ss1 due to ss1 being assigned an RS
        // prioritize(ss4, ss1) = ss4 due to ss4 winning in lexicographic ordering
        // Therefore only ss4 among these SS IDs should be retained in dbsnpSVE
        // ss5 and ss6 sharing ID ss2 will also be retained because ss6 has mismatched alleles
        List<DbsnpSubmittedVariantEntity> entriesInDbsnpSVE = this.mongoTemplate.findAll(
                DbsnpSubmittedVariantEntity.class);
        assertEquals(3, entriesInDbsnpSVE.size());
        List<String> expectedHashesInDbsnpSVE = Stream.of(ss4, ss5, ss6).map(SubmittedVariantEntity::getId)
                                                      .collect(Collectors.toList());
        assertTrue(entriesInDbsnpSVE.stream().map(DbsnpSubmittedVariantEntity::getId)
                                    .allMatch(expectedHashesInDbsnpSVE::contains));

        // other SS besides ss4 should be issued new IDs
        List<SubmittedVariantEntity> entriesInSVE = this.mongoTemplate.findAll(SubmittedVariantEntity.class);
        assertEquals(3, entriesInSVE.size());
        assertNotEquals(ss1.getAccession(),
                        this.submittedVariantAccessioningService.get(Collections.singletonList(ss1)).get(0)
                                                                .getAccession());
        assertNotEquals(ss2.getAccession(),
                        this.submittedVariantAccessioningService.get(Collections.singletonList(ss2)).get(0)
                                                                .getAccession());
        assertNotEquals(ss3.getAccession(),
                        this.submittedVariantAccessioningService.get(Collections.singletonList(ss3)).get(0)
                                                                .getAccession());

        // check for creation of 3 new SS IDs
        assertEquals(3, this.metricCompute.getCount(ClusteringMetric.SUBMITTED_VARIANTS_SS_SPLIT));

        // Ensure no split candidate operations are left unprocessed
        assertEquals(0, this.mongoTemplate.findAll(SubmittedVariantOperationEntity.class).size());

        // Ensure all the split operations are recorded
        assertEquals(3, this.mongoTemplate.findAll(DbsnpSubmittedVariantOperationEntity.class).size());
    }

    @Test
    public void testSplitWriterRestartAfterSplitCandidateRegistrationCrashes() throws Exception {
        setupSplitScenario();
        List<SubmittedVariantEntity> svesThatShareSameID = Arrays.asList(ss1, ss2, ss3, ss4);
        SSSplitWriter mockSplitWriter = Mockito.spy((SSSplitWriter) ssSplitWriter);
        doThrow(RuntimeException.class).when(mockSplitWriter).registerSplitCandidates(svesThatShareSameID);
        // invoke process - will crash when registering split candidates
        try {
            mockSplitWriter.write(svesThatShareSameID);
        }
        catch (RuntimeException ignored) {

        }
        doCallRealMethod().when(mockSplitWriter).registerSplitCandidates(svesThatShareSameID);
        // Restart process
        mockSplitWriter.write(svesThatShareSameID);
        assertPostSplitStatus();
    }

    @Test
    public void testSplitWriterRestartAfterSplitCandidateProcessingCrashes() throws Exception {
        setupSplitScenario();
        List<SubmittedVariantEntity> svesThatShareSameID = Arrays.asList(ss1, ss2, ss3, ss4);
        SSSplitWriter mockSplitWriter = Mockito.spy((SSSplitWriter) ssSplitWriter);
        doThrow(RuntimeException.class).when(mockSplitWriter).processSplitCandidates(Mockito.anyList());
        // invoke process - will crash when processing split candidates
        try {
            mockSplitWriter.write(svesThatShareSameID);
        }
        catch (RuntimeException ignored) {

        }
        doCallRealMethod().when(mockSplitWriter).processSplitCandidates(Mockito.anyList());
        // Restart process
        mockSplitWriter.write(svesThatShareSameID);
        assertPostSplitStatus();
    }

    @Test
    public void testSplitWriterRestartAfterSplitCandidateClearingCrashes() throws Exception {
        setupSplitScenario();
        List<SubmittedVariantEntity> svesThatShareSameID = Arrays.asList(ss1, ss2, ss3, ss4);
        SSSplitWriter mockSplitWriter = Mockito.spy((SSSplitWriter) ssSplitWriter);
        doThrow(RuntimeException.class).when(mockSplitWriter)
                                       .removeCurrentSSEntriesInDBForSplitCandidates(Mockito.anySet());
        // invoke process - will crash when clearing split candidates
        try {
            mockSplitWriter.write(svesThatShareSameID);
        }
        catch (RuntimeException ignored) {

        }
        doCallRealMethod().when(mockSplitWriter).removeCurrentSSEntriesInDBForSplitCandidates(Mockito.anySet());
        // Restart process
        mockSplitWriter.write(svesThatShareSameID);
        assertPostSplitStatus();
    }

    @Test
    public void testIdempotentSplitWriter() throws Exception {
        writeSplitWithoutCrashes();
        DatabaseState dbStateAfterFirstSplitWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);

        ssSplitWriter.write(Arrays.asList(ss1, ss2, ss3, ss4));
        DatabaseState dbStateAfterSecondSplitWrite = DatabaseState.getCurrentDatabaseState(this.mongoTemplate);
        assertEquals(dbStateAfterSecondSplitWrite, dbStateAfterFirstSplitWrite);
    }
}