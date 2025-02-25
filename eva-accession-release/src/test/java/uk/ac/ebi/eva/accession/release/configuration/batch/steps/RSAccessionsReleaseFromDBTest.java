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

import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration.TEST_RELEASE_ACTIVE_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration.TEST_RELEASE_DEPRECATED_ACCESSIONS_JOB;
import static uk.ac.ebi.eva.accession.release.test.configuration.BatchTestConfiguration.TEST_RELEASE_MERGED_ACCESSIONS_JOB;

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
        createRSAccFileWithAccessions(Arrays.asList(1L, 2L, 3L, 4L));
        populateDataForMergedAccessions();

        JobExecution jobExecution = jobLauncherReleaseMergedAccessions.launchJob();
        assertEquals(BatchStatus.COMPLETED, jobExecution.getStatus());

        FileInputStream fin1 = new FileInputStream(ReportPathResolver.getEvaMergedIdsReportPath(
                        inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession())
                .toFile());
        long mergedVariants = FileUtils.countNonCommentLines(fin1);
        assertEquals(2, mergedVariants);

        FileInputStream fin2 = new FileInputStream(ReportPathResolver.getEvaMergedDeprecatedIdsReportPath(
                        inputParameters.getOutputFolder(), inputParameters.getAssemblyAccession())
                .toFile());
        long mergedDeprecatedVariants = FileUtils.countNonCommentLines(fin2);
        assertEquals(1, mergedDeprecatedVariants);
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
        // cve 1 mergeInto cve 11 and 11 is active
        ClusteredVariantInactiveEntity cveInactive1 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(1L, "Hash-1",
                "GCA_000409795.2", 60711, "CM001941.2", 100001,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps1 = new ClusteredVariantOperationEntity();
        cveOps1.fill(EventType.MERGED, 1L, 11L, "Original", Arrays.asList(cveInactive1));

        // cve 2 mergeInto cve 12 but 12 is deprecated
        ClusteredVariantInactiveEntity cveInactive2 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(2L, "Hash-2",
                "GCA_000409795.2", 60711, "CM001941.2", 100002,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps2 = new ClusteredVariantOperationEntity();
        cveOps2.fill(EventType.MERGED, 2L, 12L, "Original", Arrays.asList(cveInactive2));

        ClusteredVariantInactiveEntity cveInactive12 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(12L, "Hash-12",
                "GCA_000409795.2", 60711, "CM001941.2", 100012,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps12 = new ClusteredVariantOperationEntity();
        cveOps12.fill(EventType.DEPRECATED, 12L, null, "Deprecated", Arrays.asList(cveInactive12));

        //  cve 3 mergeInto cve 13 which mergeInto cve 14 and 14 is active
        ClusteredVariantInactiveEntity cveInactive3 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(3L, "Hash-3",
                "GCA_000409795.2", 60711, "CM001941.2", 100003,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps3 = new ClusteredVariantOperationEntity();
        cveOps3.fill(EventType.MERGED, 3L, 13L, "Original", Arrays.asList(cveInactive3));
        ClusteredVariantInactiveEntity cveInactive13 = new ClusteredVariantInactiveEntity(new ClusteredVariantEntity(13L, "Hash-13",
                "GCA_000409795.2", 60711, "CM001941.2", 100013,
                VariantType.SNV, false, LocalDateTime.now(), 1));
        ClusteredVariantOperationEntity cveOps13 = new ClusteredVariantOperationEntity();
        cveOps13.fill(EventType.MERGED, 13L, 14L, "Original", Arrays.asList(cveInactive13));


        ClusteredVariantEntity cve11 = new ClusteredVariantEntity(11L, "Hash-11",
                "GCA_000409795.2", 60711, "CM001941.2", 100011,
                VariantType.SNV, false, LocalDateTime.now(), 1);
        ClusteredVariantEntity cve14 = new ClusteredVariantEntity(14L, "Hash-14",
                "GCA_000409795.2", 60711, "CM001941.2", 100014,
                VariantType.SNV, false, LocalDateTime.now(), 1);

        SubmittedVariantInactiveEntity sveInactive1 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(1L, "SVE-Hash-1",
                "GCA_000409795.2", 60711, "study1", "CM001941.2",
                100001, "A", "T", 1L, false,
                false, false, false, 1));
        SubmittedVariantInactiveEntity sveInactive2 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(2L, "SVE-Hash-2",
                "GCA_000409795.2", 60711, "study2", "CM001941.2",
                100002, "A", "T", 2L, false,
                false, false, false, 1));
        SubmittedVariantInactiveEntity sveInactive3 = new SubmittedVariantInactiveEntity(new SubmittedVariantEntity(3L, "SVE-Hash-3",
                "GCA_000409795.2", 60711, "study3", "CM001941.2",
                100003, "A", "T", 3L, false,
                false, false, false, 1));

        SubmittedVariantOperationEntity sveOps1 = new SubmittedVariantOperationEntity();
        sveOps1.fill(EventType.UPDATED, 1L, null, "Original", Arrays.asList(sveInactive1));
        SubmittedVariantOperationEntity sveOps2 = new SubmittedVariantOperationEntity();
        sveOps2.fill(EventType.UPDATED, 2L, null, "Original", Arrays.asList(sveInactive2));
        SubmittedVariantOperationEntity sveOps3 = new SubmittedVariantOperationEntity();
        sveOps3.fill(EventType.UPDATED, 2L, null, "Original", Arrays.asList(sveInactive3));

        mongoTemplate.insert(Arrays.asList(cveOps1, cveOps2, cveOps12, cveOps3, cveOps13), ClusteredVariantOperationEntity.class);
        mongoTemplate.insert(Arrays.asList(cve11, cve14), ClusteredVariantEntity.class);
        mongoTemplate.insert(Arrays.asList(sveOps1, sveOps2, sveOps3), SubmittedVariantOperationEntity.class);
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
