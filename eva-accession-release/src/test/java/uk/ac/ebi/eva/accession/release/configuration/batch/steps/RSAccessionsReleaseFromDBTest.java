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
import org.springframework.batch.core.BatchStatus;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.test.JobLauncherTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.parameters.ReportPathResolver;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.ReleaseFromDBTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.utils.FileUtils;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.test.configuration.ReleaseFromDBTestConfiguration.TEST_RELEASE_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.test.configuration.ReleaseFromDBTestConfiguration.TEST_RELEASE_DEPRECATED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.test.configuration.ReleaseFromDBTestConfiguration.TEST_RELEASE_MERGED_ACCESSIONS_JOB;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {MongoTestConfiguration.class, ReleaseFromDBTestConfiguration.class})
@TestPropertySource("classpath:dump-rs-accession-test.properties")
public class RSAccessionsReleaseFromDBTest {
    private static final String TEST_DB = "test-db";
    private static final String testRunDir = "src/test/resources/release-test-run";
    private static final String rsAccFile = "src/test/resources/release-test-run/rsAccFile.csv";

    @Autowired
    InputParameters inputParameters;

    @Autowired
    @Qualifier(TEST_RELEASE_ACTIVE_ACCESSIONS_JOB)
    private JobLauncherTestUtils jobLauncherReleaseActiveAccessions;

    @Autowired
    @Qualifier(TEST_RELEASE_DEPRECATED_ACCESSIONS_JOB)
    private JobLauncherTestUtils jobLauncherReleaseDeprecatedAccessions;

    @Autowired
    @Qualifier(TEST_RELEASE_MERGED_ACCESSIONS_JOB)
    private JobLauncherTestUtils jobLauncherReleaseMergedAccessions;

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
        if (Files.exists(Paths.get(testRunDir))) {
            deleteDirectory(Paths.get(testRunDir));
        }
        Files.createDirectories(Paths.get(testRunDir));
    }

    @After
    public void tearDown() throws IOException {
        this.mongoClient.dropDatabase(TEST_DB);
        if (Files.exists(Paths.get(testRunDir))) {
            deleteDirectory(Paths.get(testRunDir));
        }
    }

    @Test
    public void testReleaseActiveRSAccessionsStep() throws Exception {
        createRSAccFileWithAccessions(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L));
        populateDataForActiveAccessions();

        JobExecution jobExecution = jobLauncherReleaseActiveAccessions.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        FileInputStream fin = new FileInputStream(ReportPathResolver.getEvaCurrentIdsReportPath(
                        inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession())
                .toFile());
        long numVariantsInRelease = FileUtils.countNonCommentLines(fin);
        assertEquals(6, numVariantsInRelease);
    }

    @Test
    public void testReleaseDeprecatedRSAccessionsStep() throws Exception {
        createRSAccFileWithAccessions(Arrays.asList(1L, 2L, 3L, 4L));
        populateDataForDeprecatedAccessions();

        JobExecution jobExecution = jobLauncherReleaseDeprecatedAccessions.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        FileInputStream fin = new FileInputStream(ReportPathResolver.getEvaDeprecatedIdsReportPath(
                        inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession())
                .toFile());
        long numVariantsInRelease = FileUtils.countNonCommentLines(fin);
        assertEquals(4, numVariantsInRelease);
    }

    @Test
    public void testReleaseMergedRSAccessionsStep() throws Exception {
        createRSAccFileWithAccessions(Arrays.asList(1L, 2L, 3L, 4L, 5L, 6L, 7L, 8L));
        populateDataForMergedAccessions();

        JobExecution jobExecution = jobLauncherReleaseMergedAccessions.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        FileInputStream mergedDeprecatedInputStream = new FileInputStream(ReportPathResolver.getEvaMergedDeprecatedIdsReportPath(
                        inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession())
                .toFile());
        List<String> mergedDeprecatedRSIdsList = new BufferedReader(new InputStreamReader(mergedDeprecatedInputStream))
                .lines().collect(Collectors.toList());
        assertEquals(3, mergedDeprecatedRSIdsList.size());
        assertEquals("rs1", mergedDeprecatedRSIdsList.get(0));
        assertEquals("rs2", mergedDeprecatedRSIdsList.get(1));
        assertEquals("rs8", mergedDeprecatedRSIdsList.get(2));


        FileInputStream mergedInputStream = new FileInputStream(ReportPathResolver.getEvaMergedIdsReportPath(
                        inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession())
                .toFile());
        long mergedVariants = FileUtils.countNonCommentLines(mergedInputStream);
        assertEquals(4, mergedVariants);
    }

    public void populateDataForActiveAccessions() {
        ClusteredVariantEntity cve1 = new ClusteredVariantEntity(1L, "CVE-Hash-1",
                "GCA_000409795.2", 60711, "CM001941.2", 100001,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        ClusteredVariantEntity cve2 = new ClusteredVariantEntity(2L, "CVE-Hash-2",
                "GCA_000409795.2", 60711, "CM001941.2", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        ClusteredVariantEntity cve3 = new ClusteredVariantEntity(3L, "CVE-Hash-3",
                "GCA_000409795.2", 60711, "CM001941.2", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        DbsnpClusteredVariantEntity dbsnpCve1 = new DbsnpClusteredVariantEntity(4L, "CVE-Hash-4",
                "GCA_000409795.2", 60711, "CM001941.2", 100004,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        DbsnpClusteredVariantEntity dbsnpCve2 = new DbsnpClusteredVariantEntity(5L, "CVE-Hash-5",
                "GCA_000409795.2", 60711, "CM001941.2", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        DbsnpClusteredVariantEntity dbsnpCve3 = new DbsnpClusteredVariantEntity(6L, "CVE-Hash-6",
                "GCA_000409795.2", 60711, "CM001941.2", 100006,
                VariantType.SNV, false, LocalDateTime.now(), 1);


        SubmittedVariantEntity sve1 = new SubmittedVariantEntity(1L, "SVE-Hash-1",
                "GCA_000409795.2", 60711, "study1", "CM001941.2",
                100001, "A", "T", 1L, false,
                false, false, false, 1);
        SubmittedVariantEntity sve2 = new SubmittedVariantEntity(2L, "SVE-Hash-2",
                "GCA_000409795.2", 60711, "study2", "CM001941.2",
                100002, "A", "T", 2L, false,
                false, false, false, 1);
        SubmittedVariantEntity sve3 = new SubmittedVariantEntity(3L, "SVE-Hash-3",
                "GCA_000409795.2", 60711, "study3", "CM001941.2",
                100003, "A", "T", 3L, false,
                false, false, false, 1);

        DbsnpSubmittedVariantEntity dbsnpSve1 = new DbsnpSubmittedVariantEntity(4L, "SVE-Hash-4",
                "GCA_000409795.2", 60711, "study4", "CM001941.2",
                100004, "A", "T", 4L, false,
                false, false, false, 1);
        DbsnpSubmittedVariantEntity dbsnpSve2 = new DbsnpSubmittedVariantEntity(5L, "SVE-Hash-5",
                "GCA_000409795.2", 60711, "study5", "CM001941.2",
                100005, "A", "T", 5L, false,
                false, false, false, 1);
        DbsnpSubmittedVariantEntity dbsnpSve3 = new DbsnpSubmittedVariantEntity(6L, "SVE-Hash-6",
                "GCA_000409795.2", 60711, "study6", "CM001941.2",
                100006, "A", "T", 6L, false,
                false, false, false, 1);

        mongoTemplate.insert(Arrays.asList(cve1, cve2, cve3), ClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(dbsnpCve1, dbsnpCve2, dbsnpCve3), DbsnpClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(sve1, sve2, sve3), SubmittedVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(dbsnpSve1, dbsnpSve2, dbsnpSve3), DbsnpSubmittedVariantEntity.class);
    }

    public void populateDataForDeprecatedAccessions() {
        ClusteredVariantInactiveEntity cveInactive1 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(1L, "Hash-1",
                "GCA_000409795.2", 60711, "CM001941.2", 100001,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantInactiveEntity cveInactive2 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(2L, "Hash-2",
                "GCA_000409795.2", 60711, "CM001941.2", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps1 = new ClusteredVariantOperationEntity();
        cveOps1.fill(EventType.DEPRECATED, 1L, null, "DEPRECATED", Arrays.asList(cveInactive1));
        ClusteredVariantOperationEntity cveOps2 = new ClusteredVariantOperationEntity();
        cveOps2.fill(EventType.DEPRECATED, 2L, null, "DEPRECATED", Arrays.asList(cveInactive2));

        DbsnpClusteredVariantInactiveEntity dbsnpCveInactive1 = new DbsnpClusteredVariantInactiveEntity(new DbsnpClusteredVariantEntity(3L, "Hash-3",
                "GCA_000409795.2", 60711, "CM001941.2", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        DbsnpClusteredVariantInactiveEntity dbsnpCveInactive2 = new DbsnpClusteredVariantInactiveEntity(new DbsnpClusteredVariantEntity(4L, "Hash-4",
                "GCA_000409795.2", 60711, "CM001941.2", 100004,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity dbsnpCveOps1 = new ClusteredVariantOperationEntity();
        dbsnpCveOps1.fill(EventType.DEPRECATED, 3L, null, "DEPRECATED", Arrays.asList(dbsnpCveInactive1));
        ClusteredVariantOperationEntity dbsnpCveOps2 = new ClusteredVariantOperationEntity();
        dbsnpCveOps2.fill(EventType.DEPRECATED, 4L, null, "DEPRECATED", Arrays.asList(dbsnpCveInactive2));

        SubmittedVariantInactiveEntity sveInactive1 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(1L, "SVE-Hash-1",
                "GCA_000409795.2", 60711, "study1", "CM001941.2",
                100001, "A", "T", 1L, false,
                false, false, false, 1));
        SubmittedVariantInactiveEntity sveInactive2 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(2L, "SVE-Hash-2",
                "GCA_000409795.2", 60711, "study2", "CM001941.2",
                100002, "A", "T", 2L, false,
                false, false, false, 1));
        SubmittedVariantOperationEntity sveOps1 = new SubmittedVariantOperationEntity();
        sveOps1.fill(EventType.UPDATED, 1L, null, "UPDATED", Arrays.asList(sveInactive1));
        SubmittedVariantOperationEntity sveOps2 = new SubmittedVariantOperationEntity();
        sveOps2.fill(EventType.UPDATED, 2L, null, "UPDATED", Arrays.asList(sveInactive2));

        DbsnpSubmittedVariantInactiveEntity dbsnpSveInactive1 = new DbsnpSubmittedVariantInactiveEntity(new DbsnpSubmittedVariantEntity(3L, "SVE-Hash-3",
                "GCA_000409795.2", 60711, "study3", "CM001941.2",
                100003, "A", "T", 3L, false,
                false, false, false, 1));
        DbsnpSubmittedVariantInactiveEntity dbsnpSveInactive2 = new DbsnpSubmittedVariantInactiveEntity(new DbsnpSubmittedVariantEntity(4L, "SVE-Hash-4",
                "GCA_000409795.2", 60711, "study4", "CM001941.2",
                100004, "A", "T", 4L, false,
                false, false, false, 1));
        DbsnpSubmittedVariantOperationEntity dbsnpSveOps1 = new DbsnpSubmittedVariantOperationEntity();
        dbsnpSveOps1.fill(EventType.UPDATED, 3L, null, "UPDATED", Arrays.asList(dbsnpSveInactive1));
        DbsnpSubmittedVariantOperationEntity dbsnpSveOps2 = new DbsnpSubmittedVariantOperationEntity();
        dbsnpSveOps2.fill(EventType.UPDATED, 4L, null, "UPDATED", Arrays.asList(dbsnpSveInactive2));

        mongoTemplate.insert(Arrays.asList(cveOps1, cveOps2), ClusteredVariantOperationEntity.class);
        mongoTemplate.insert(Arrays.asList(dbsnpCveOps1, dbsnpCveOps2), DbsnpClusteredVariantOperationEntity.class);
        mongoTemplate.insert(Arrays.asList(sveOps1, sveOps2), SubmittedVariantOperationEntity.class);
        mongoTemplate.insert(Arrays.asList(dbsnpSveOps1, dbsnpSveOps2), DbsnpSubmittedVariantOperationEntity.class);
    }


    public void populateDataForMergedAccessions() {
        List<ClusteredVariantEntity> cveList = new ArrayList<>();
        List<ClusteredVariantOperationEntity> cveOpsList = new ArrayList<>();

        // cve 1 mergeInto cve 11
        // cve 11 is Deprecated
        ClusteredVariantInactiveEntity cveInactive_1 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(1L, "Hash-1",
                "GCA_000409795.2", 60711, "CM001941.2", 100001,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_1_11 = new ClusteredVariantOperationEntity();
        cveOps_Merge_1_11.fill(EventType.MERGED, 1L, 11L, "Original", Arrays.asList(cveInactive_1));

        ClusteredVariantInactiveEntity cveInactive_11 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(11L, "Hash-11",
                "GCA_000409795.2", 60711, "CM001941.2", 100001,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Deprecate_11 = new ClusteredVariantOperationEntity();
        cveOps_Deprecate_11.fill(EventType.DEPRECATED, 11L, null, "Deprecated", Arrays.asList(cveInactive_11));

        cveOpsList.add(cveOps_Merge_1_11);
        cveOpsList.add(cveOps_Deprecate_11);

        // cve 2 mergeInto cve 21
        // cve 21 merged into cve 211
        // cve 211 is Deprecated
        ClusteredVariantInactiveEntity cveInactive_2 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(2L, "Hash-2",
                "GCA_000409795.2", 60711, "CM001941.2", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_2_21 = new ClusteredVariantOperationEntity();
        cveOps_Merge_2_21.fill(EventType.MERGED, 2L, 21L, "Original", Arrays.asList(cveInactive_2));

        ClusteredVariantInactiveEntity cveInactive_21 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(21L, "Hash-21",
                "GCA_000409795.2", 60711, "CM001941.2", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_21_211 = new ClusteredVariantOperationEntity();
        cveOps_Merge_21_211.fill(EventType.MERGED, 21L, 211L, "Original", Arrays.asList(cveInactive_21));

        ClusteredVariantInactiveEntity cveInactive_211 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(211L, "Hash-211",
                "GCA_000409795.2", 60711, "CM001941.2", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Deprecate_211 = new ClusteredVariantOperationEntity();
        cveOps_Deprecate_211.fill(EventType.DEPRECATED, 211L, null, "Deprecated", Arrays.asList(cveInactive_211));

        cveOpsList.add(cveOps_Merge_2_21);
        cveOpsList.add(cveOps_Merge_21_211);
        cveOpsList.add(cveOps_Deprecate_211);

        // cve 3 mergeInto cve 31
        // cve 31 mergeInto cve 311,312
        // cve 311 is Active
        // cve 312 mergedInto cve 31
        ClusteredVariantInactiveEntity cveInactive_3 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(3L, "Hash-3",
                "GCA_000409795.2", 60711, "CM001941.2", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_3_31 = new ClusteredVariantOperationEntity();
        cveOps_Merge_3_31.fill(EventType.MERGED, 3L, 31L, "Original", Arrays.asList(cveInactive_3));

        ClusteredVariantInactiveEntity cveInactive_31 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(31L, "Hash-31",
                "GCA_000409795.2", 60711, "CM001941.2", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_31_311 = new ClusteredVariantOperationEntity();
        cveOps_Merge_31_311.fill(EventType.MERGED, 31L, 311L, "Original", Arrays.asList(cveInactive_31));
        ClusteredVariantOperationEntity cveOps_Merge_31_312 = new ClusteredVariantOperationEntity();
        cveOps_Merge_31_312.fill(EventType.MERGED, 31L, 312L, "Original", Arrays.asList(cveInactive_31));

        ClusteredVariantInactiveEntity cveInactive_312 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(312L, "Hash-312",
                "GCA_000409795.2", 60711, "CM001941.2", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_312_31 = new ClusteredVariantOperationEntity();
        cveOps_Merge_312_31.fill(EventType.MERGED, 312L, 31L, "Original", Arrays.asList(cveInactive_312));

        ClusteredVariantEntity cve_311 = new ClusteredVariantEntity(311L, "Hash-311",
                "GCA_000409795.2", 60711, "CM001941.2", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        cveOpsList.add(cveOps_Merge_3_31);
        cveOpsList.add(cveOps_Merge_31_311);
        cveOpsList.add(cveOps_Merge_31_312);
        cveOpsList.add(cveOps_Merge_312_31);
        cveList.add(cve_311);

        // cve 4 mergeInto cve 41
        // cve 41 is Active
        ClusteredVariantInactiveEntity cveInactive_4 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(4L, "Hash-4",
                "GCA_000409795.2", 60711, "CM001941.2", 100004,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_4_41 = new ClusteredVariantOperationEntity();
        cveOps_Merge_4_41.fill(EventType.MERGED, 4L, 41L, "Original", Arrays.asList(cveInactive_4));

        ClusteredVariantEntity cve_41 = new ClusteredVariantEntity(41L, "Hash-41",
                "GCA_000409795.2", 60711, "CM001941.2", 100004,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        cveOpsList.add(cveOps_Merge_4_41);
        cveList.add(cve_41);

        // cve 5 mergeInto cve 51
        // cve 51 mergeInto cve 511,512
        // cve 512 mergeInto 5121
        // cve 511 is active
        // cve 5121 is Active
        ClusteredVariantInactiveEntity cveInactive_5 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(5L, "Hash-5",
                "GCA_000409795.2", 60711, "CM001941.2", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_5_51 = new ClusteredVariantOperationEntity();
        cveOps_Merge_5_51.fill(EventType.MERGED, 5L, 51L, "Original", Arrays.asList(cveInactive_5));

        ClusteredVariantInactiveEntity cveInactive_51 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(51L, "Hash-51",
                "GCA_000409795.2", 60711, "CM001941.2", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_51_511 = new ClusteredVariantOperationEntity();
        cveOps_Merge_51_511.fill(EventType.MERGED, 51L, 511L, "Original", Arrays.asList(cveInactive_51));
        ClusteredVariantOperationEntity cveOps_Merge_51_512 = new ClusteredVariantOperationEntity();
        cveOps_Merge_51_512.fill(EventType.MERGED, 51L, 512L, "Original", Arrays.asList(cveInactive_51));

        ClusteredVariantInactiveEntity cveInactive_512 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(512L, "Hash-512",
                "GCA_000409795.2", 60711, "CM001941.2", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_512_5121 = new ClusteredVariantOperationEntity();
        cveOps_Merge_512_5121.fill(EventType.MERGED, 512L, 5121L, "Original", Arrays.asList(cveInactive_512));

        ClusteredVariantEntity cve_511 = new ClusteredVariantEntity(511L, "Hash-511",
                "GCA_000409795.2", 60711, "CM001941.2", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        ClusteredVariantEntity cve_5121 = new ClusteredVariantEntity(5121L, "Hash-5121",
                "GCA_000409795.2", 60711, "CM001941.2", 100005,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        cveOpsList.add(cveOps_Merge_5_51);
        cveOpsList.add(cveOps_Merge_51_511);
        cveOpsList.add(cveOps_Merge_51_512);
        cveOpsList.add(cveOps_Merge_512_5121);
        cveList.add(cve_511);
        cveList.add(cve_5121);

        // cve 6 mergeInto cve 61
        // cve 61 is neither active nor has any further operations
        ClusteredVariantInactiveEntity cveInactive_6 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(6L, "Hash-6",
                "GCA_000409795.2", 60711, "CM001941.2", 100006,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_6_61 = new ClusteredVariantOperationEntity();
        cveOps_Merge_6_61.fill(EventType.MERGED, 6L, 61L, "Original", Arrays.asList(cveInactive_6));

        cveOpsList.add(cveOps_Merge_6_61);

        // cve 7 mergeInto cve 71,72
        // cve 71 is Active
        // cve 72 is neither active nor has any further operations
        ClusteredVariantInactiveEntity cveInactive_7 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(7L, "Hash-7",
                "GCA_000409795.2", 60711, "CM001941.2", 100007,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Merge_7_71 = new ClusteredVariantOperationEntity();
        cveOps_Merge_7_71.fill(EventType.MERGED, 7L, 71L, "Original", Arrays.asList(cveInactive_7));
        ClusteredVariantOperationEntity cveOps_Merge_7_72 = new ClusteredVariantOperationEntity();
        cveOps_Merge_7_72.fill(EventType.MERGED, 7L, 72L, "Original", Arrays.asList(cveInactive_7));

        ClusteredVariantEntity cve_71 = new ClusteredVariantEntity(71L, "Hash-71",
                "GCA_000409795.2", 60711, "CM001941.2", 100007,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        cveOpsList.add(cveOps_Merge_7_71);
        cveOpsList.add(cveOps_Merge_7_72);
        cveList.add(cve_71);

        // cve 8 is deprecated and has no merged operations
        ClusteredVariantInactiveEntity cveInactive_8 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(8L, "Hash-8",
                "GCA_000409795.2", 60711, "CM001941.2", 100008,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps_Deprecate_8 = new ClusteredVariantOperationEntity();
        cveOps_Deprecate_8.fill(EventType.DEPRECATED, 8L, null, "Deprecated", Arrays.asList(cveInactive_8));
        cveOpsList.add(cveOps_Deprecate_8);


        SubmittedVariantInactiveEntity sveInactive_3 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(3L, "SVE-Hash-3",
                "GCA_000409795.2", 60711, "study3", "CM001941.2",
                100003, "A", "T", 3L, false,
                false, false, false, 1));
        SubmittedVariantInactiveEntity sveInactive_4 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(4L, "SVE-Hash-4",
                "GCA_000409795.2", 60711, "study4", "CM001941.2",
                100004, "A", "T", 4L, false,
                false, false, false, 1));
        SubmittedVariantInactiveEntity sveInactive_5 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(5L, "SVE-Hash-5",
                "GCA_000409795.2", 60711, "study5", "CM001941.2",
                100005, "A", "T", 5L, false,
                false, false, false, 1));
        SubmittedVariantInactiveEntity sveInactive_7 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(7L, "SVE-Hash-7",
                "GCA_000409795.2", 60711, "study7", "CM001941.2",
                100007, "A", "T", 7L, false,
                false, false, false, 1));

        SubmittedVariantOperationEntity sveOps_3 = new SubmittedVariantOperationEntity();
        sveOps_3.fill(EventType.UPDATED, 3L, null, "Original", Arrays.asList(sveInactive_3));
        SubmittedVariantOperationEntity sveOps_4 = new SubmittedVariantOperationEntity();
        sveOps_4.fill(EventType.UPDATED, 4L, null, "Original", Arrays.asList(sveInactive_4));
        SubmittedVariantOperationEntity sveOps_5 = new SubmittedVariantOperationEntity();
        sveOps_5.fill(EventType.UPDATED, 5L, null, "Original", Arrays.asList(sveInactive_5));
        SubmittedVariantOperationEntity sveOps_7 = new SubmittedVariantOperationEntity();
        sveOps_7.fill(EventType.UPDATED, 7L, null, "Original", Arrays.asList(sveInactive_7));

        mongoTemplate.insert(cveOpsList, ClusteredVariantOperationEntity.class);
        mongoTemplate.insert(cveList, ClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(sveOps_3, sveOps_4, sveOps_5, sveOps_7), SubmittedVariantOperationEntity.class);
    }


    public void createRSAccFileWithAccessions(List<Long> accList) throws IOException {
        Files.deleteIfExists(Paths.get(rsAccFile));
        try (BufferedWriter br = new BufferedWriter(new FileWriter(rsAccFile))) {
            for (Long acc : accList) {
                br.write(acc + "\n");
            }
        }
    }

    public void assertDumpRSAccFileContains(String filePath, Set<Long> expectedAccSet) throws IOException {
        Set<Long> rsAccSet = Files.lines(Paths.get(filePath))
                .map(l -> Long.parseLong(l))
                .collect(Collectors.toSet());

        assertEquals(expectedAccSet, rsAccSet);
    }

    public static void deleteDirectory(Path path) throws IOException {
        Files.walkFileTree(path, new SimpleFileVisitor<Path>() {
            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }
}
