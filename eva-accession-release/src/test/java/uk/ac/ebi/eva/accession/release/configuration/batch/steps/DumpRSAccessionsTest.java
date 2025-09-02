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

package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.Job;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.ReleaseFromDBTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.test.configuration.ReleaseFromDBTestConfiguration.TEST_DUMP_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.test.configuration.ReleaseFromDBTestConfiguration.TEST_DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {MongoTestConfiguration.class, ReleaseFromDBTestConfiguration.class})
@TestPropertySource("classpath:dump-rs-accession-test.properties")
public class DumpRSAccessionsTest {
    private static final String TEST_DB = "test-db";
    private static final String accDumpFile = "src/test/resources/accDumpFile.csv";

    @Autowired
    @Qualifier(TEST_DUMP_ACTIVE_ACCESSIONS_JOB)
    private JobLauncherTestUtils jobLauncherDumpActiveAccessions;

    @Autowired
    @Qualifier(TEST_DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB)
    private JobLauncherTestUtils jobLauncherDumpMergedAndDeprecatedAccessions;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    @Before
    public void setUp() throws IOException {
        this.mongoClient.dropDatabase(TEST_DB);
        Files.deleteIfExists(Paths.get(accDumpFile));
    }

    @After
    public void tearDown() throws IOException {
        this.mongoClient.dropDatabase(TEST_DB);
        Files.deleteIfExists(Paths.get(accDumpFile));
    }

    @Test
    public void testDumpActiveRSAccessionsStep() throws Exception {
        populateDataForActiveAccessions();
        jobLauncherDumpActiveAccessions.launchJob();

        Set<Long> expectedAccSet = new HashSet<>(Arrays.asList(1L, 2L, 4L, 5L));
        assertDumpRSAccFileContains(expectedAccSet);
    }

    @Test
    public void testDumpMergedAndDeprecatedRSAccessionsStep() throws Exception {
        populateDataForMergedAndDeprecatedAccessions();
        jobLauncherDumpMergedAndDeprecatedAccessions.launchJob();

        Set<Long> expectedAccSet = new HashSet<>(Arrays.asList(1L, 2L, 5L, 6L, 21L, 22L, 25L, 26L));
        assertDumpRSAccFileContains(expectedAccSet);
    }

    public void populateDataForActiveAccessions() {
        ClusteredVariantEntity cve1 = new ClusteredVariantEntity(1L, "Hash-1",
                "GCA_000409795.2", 60711, "contig", 100001,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        ClusteredVariantEntity cve2 = new ClusteredVariantEntity(2L, "Hash-2",
                "GCA_000409795.2", 60711, "contig", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        ClusteredVariantEntity cve3 = new ClusteredVariantEntity(3L, "Hash-3",
                "GCA_000409795.3", 60711, "contig", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        DbsnpClusteredVariantEntity dbsnp1 = new DbsnpClusteredVariantEntity(4L, "Hash-4",
                "GCA_000409795.2", 60711, "contig", 100004,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        DbsnpClusteredVariantEntity dbsnp2 = new DbsnpClusteredVariantEntity(5L, "Hash-5",
                "GCA_000409795.2", 60711, "contig", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        DbsnpClusteredVariantEntity dbsnp3 = new DbsnpClusteredVariantEntity(6L, "Hash-6",
                "GCA_000409795.3", 60711, "contig", 100006,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        mongoTemplate.insert(Arrays.asList(cve1, cve2, cve3), ClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(dbsnp1, dbsnp2, dbsnp3), DbsnpClusteredVariantEntity.class);
    }

    public void populateDataForMergedAndDeprecatedAccessions() {
        // data for merged accessions
        ClusteredVariantInactiveEntity cveInactive1 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(1L, "Hash-1",
                "GCA_000409795.2", 60711, "contig", 100001,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantInactiveEntity cveInactive2 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(2L, "Hash-2",
                "GCA_000409795.2", 60711, "contig", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps1 = new ClusteredVariantOperationEntity();
        cveOps1.fill(EventType.MERGED, 1L, 3L, "MERGED", Arrays.asList(cveInactive1));
        ClusteredVariantOperationEntity cveOps2 = new ClusteredVariantOperationEntity();
        cveOps2.fill(EventType.MERGED, 2L, 4L, "MERGED", Arrays.asList(cveInactive2));

        DbsnpClusteredVariantInactiveEntity dbsnpInactive5 = new DbsnpClusteredVariantInactiveEntity(new DbsnpClusteredVariantEntity(5L, "Hash-5",
                "GCA_000409795.2", 60711, "contig", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        DbsnpClusteredVariantInactiveEntity dbsnpInactive6 = new DbsnpClusteredVariantInactiveEntity(new DbsnpClusteredVariantEntity(6L, "Hash-6",
                "GCA_000409795.2", 60711, "contig", 100006,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity dbsnpOps5 = new ClusteredVariantOperationEntity();
        dbsnpOps5.fill(EventType.MERGED, 5L, 7L, "MERGED", Arrays.asList(dbsnpInactive5));
        ClusteredVariantOperationEntity dbsnpOps6 = new ClusteredVariantOperationEntity();
        dbsnpOps6.fill(EventType.MERGED, 6L, 8L, "MERGED", Arrays.asList(dbsnpInactive6));

        // data for active accessions
        ClusteredVariantEntity cve10 = new ClusteredVariantEntity(10L, "Hash-10",
                "GCA_000409795.2", 60711, "contig", 100010,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        ClusteredVariantEntity cve11 = new ClusteredVariantEntity(11L, "Hash-11",
                "GCA_000409795.2", 60711, "contig", 100011,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        // data for deprecated accessions
        ClusteredVariantInactiveEntity cveInactive21 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(21L, "Hash-21",
                "GCA_000409795.2", 60711, "contig", 100021,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantInactiveEntity cveInactive22 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(22L, "Hash-22",
                "GCA_000409795.2", 60711, "contig", 100022,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps21 = new ClusteredVariantOperationEntity();
        cveOps21.fill(EventType.DEPRECATED, 21L, 23L, "DEPRECATED", Arrays.asList(cveInactive21));
        ClusteredVariantOperationEntity cveOps22 = new ClusteredVariantOperationEntity();
        cveOps22.fill(EventType.DEPRECATED, 22L, 24L, "DEPRECATED", Arrays.asList(cveInactive22));

        DbsnpClusteredVariantInactiveEntity dbsnpInactive25 = new DbsnpClusteredVariantInactiveEntity(new DbsnpClusteredVariantEntity(25L, "Hash-25",
                "GCA_000409795.2", 60711, "contig", 100025,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        DbsnpClusteredVariantInactiveEntity dbsnpInactive26 = new DbsnpClusteredVariantInactiveEntity(new DbsnpClusteredVariantEntity(26L, "Hash-26",
                "GCA_000409795.2", 60711, "contig", 100026,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity dbsnpOps25 = new ClusteredVariantOperationEntity();
        dbsnpOps25.fill(EventType.DEPRECATED, 25L, 27L, "DEPRECATED", Arrays.asList(dbsnpInactive25));
        ClusteredVariantOperationEntity dbsnpOps26 = new ClusteredVariantOperationEntity();
        dbsnpOps26.fill(EventType.DEPRECATED, 26L, 28L, "DEPRECATED", Arrays.asList(dbsnpInactive26));

        mongoTemplate.insert(Arrays.asList(cveOps1, cveOps2, cveOps21, cveOps22), ClusteredVariantOperationEntity.class);
        mongoTemplate.insert(Arrays.asList(dbsnpOps5, dbsnpOps6, dbsnpOps25, dbsnpOps26), DbsnpClusteredVariantOperationEntity.class);
        mongoTemplate.insert(Arrays.asList(cve10, cve11), ClusteredVariantEntity.class);
    }

    public void assertDumpRSAccFileContains(Set<Long> expectedAccSet) throws IOException {
        Set<Long> rsAccSet = Files.lines(Paths.get(accDumpFile))
                .map(l -> Long.parseLong(l))
                .collect(Collectors.toSet());

        assertEquals(expectedAccSet, rsAccSet);
    }


    @Bean(TEST_DUMP_ACTIVE_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsActiveAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(DUMP_ACTIVE_ACCESSIONS_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

    @Bean(TEST_DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB)
    public JobLauncherTestUtils jobLauncherTestUtilsMergedAccessions() {
        return new JobLauncherTestUtils() {
            @Override
            @Autowired
            public void setJob(@Qualifier(DUMP_MERGED_AND_DEPRECATED_ACCESSIONS_JOB) Job job) {
                super.setJob(job);
            }
        };
    }

}
