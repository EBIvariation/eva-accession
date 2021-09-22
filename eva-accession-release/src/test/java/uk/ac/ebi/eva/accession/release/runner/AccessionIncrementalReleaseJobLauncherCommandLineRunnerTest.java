/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.release.runner;

import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import htsjdk.variant.variantcontext.VariantContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.test.JobRepositoryTestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoOperations;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.core.batch.io.AccessionedVcfLineMapper;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.ReleaseRecordEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.ClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.eva.SubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.release.batch.io.ReleaseRecordWriter;
import uk.ac.ebi.eva.accession.release.test.configuration.IncrementalReleaseTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import javax.sql.DataSource;
import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.CREATE_INCREMENTAL_ACCESSION_RELEASE_JOB;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_PROCESSOR;
import static uk.ac.ebi.eva.accession.release.runner.AccessionReleaseJobLauncherCommandLineRunner.EXIT_WITHOUT_ERRORS;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:incremental-release-pipeline-test.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class,
        IncrementalReleaseTestConfiguration.class})
public class AccessionIncrementalReleaseJobLauncherCommandLineRunnerTest {

    private static final String ASSEMBLY_ACCESSION = "GCA_000409795.2";

    private static final String TEST_DB = "test-db";

    private ExecutionContext executionContext;

    @Autowired
    @Qualifier(RELEASE_PROCESSOR)
    private ItemProcessor<Variant, VariantContext> releaseProcessor;

    @Autowired
    private JobRepository jobRepository;

    @Autowired
    private DataSource datasource;

    @Autowired
    private AccessionReleaseJobLauncherCommandLineRunner runner;

    @Autowired
    private MongoOperations mongoOperations;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private ClusteredVariantAccessioningRepository clusteredVariantAccessioningRepository;

    @Autowired
    private SubmittedVariantAccessioningRepository submittedVariantAccessioningRepository;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private SubmittedVariantEntity createSSFromVariant(VariantContext variant, Long rsAccession) {
        Long ssAccession = Long.parseLong(variant.getID().substring(2));
        return new SubmittedVariantEntity(ssAccession, "hash" + ssAccession, ASSEMBLY_ACCESSION, 60711,
                                          "PRJ1", variant.getContig(),
                                          variant.getStart(), variant.getReference().getBaseString(),
                                          variant.getAlternateAllele(0).getBaseString(),
                                          rsAccession, false, false, false, false, 1);
    }

    private ClusteredVariantEntity createRSFromVariant(VariantContext variant, Long rsAccession) {
        return new ClusteredVariantEntity(rsAccession, "hash" + rsAccession, ASSEMBLY_ACCESSION, 60711,
                                          variant.getContig(), variant.getStart(), VariantType.SNV, false, null, 1);
    }

    private List<VariantContext> getVariants(String accessionedVcfFilePath) throws Exception {
        VcfReader vcfReader = new VcfReader(new AccessionedVcfLineMapper(), new File(accessionedVcfFilePath));
        vcfReader.open(executionContext);
        List<Variant> variants = vcfReader.read();
        variants.addAll(vcfReader.read());
        variants.addAll(vcfReader.read());
        vcfReader.close();
        List<VariantContext> results = new ArrayList<>();
        for (Variant variant: variants) {
            results.add(releaseProcessor.process(variant));
        }
        return results;
    }

    @Before
    public void setUp() throws Exception {
        executionContext = new ExecutionContext();
        setupJobRepository();
        cleanupDatabase();

        List<VariantContext> study1Variants = getVariants(
                "src/test/resources/test-data/incremental_release/study1.accessioned.vcf");
        Long evaRS1_accession = 3000000001L;
        ClusteredVariantEntity evaRS1 = createRSFromVariant(study1Variants.get(0), evaRS1_accession);
        SubmittedVariantEntity evaSS1 = createSSFromVariant(study1Variants.get(0), evaRS1_accession);
        Long evaRS2_accession = 3000000002L;
        ClusteredVariantEntity evaRS2 = createRSFromVariant(study1Variants.get(1), evaRS2_accession);
        SubmittedVariantEntity evaSS2 = createSSFromVariant(study1Variants.get(1), evaRS2_accession);
        Long evaRS3_accession = 3000000003L;
        ClusteredVariantEntity evaRS3 = createRSFromVariant(study1Variants.get(2), evaRS3_accession);
        SubmittedVariantEntity evaSS3 = createSSFromVariant(study1Variants.get(2), evaRS3_accession);

        clusteredVariantAccessioningRepository.saveAll(Arrays.asList(evaRS1, evaRS2, evaRS3));
        submittedVariantAccessioningRepository.saveAll(Arrays.asList(evaSS1, evaSS2, evaSS3));
    }

    @After
    public void cleanupDatabase() {
        submittedVariantAccessioningRepository.deleteAll();
        clusteredVariantAccessioningRepository.deleteAll();
        mongoOperations.dropCollection(ReleaseRecordWriter.RELEASE_RECORD_COLLECTION_NAME);
    }

    private void setupJobRepository() {
        JobRepositoryTestUtils jobRepositoryTestUtils = new JobRepositoryTestUtils(jobRepository, datasource);
        runner.setJobNames(CREATE_INCREMENTAL_ACCESSION_RELEASE_JOB);
        jobRepositoryTestUtils.removeJobExecutions();
    }

    @Test
    @DirtiesContext
    public void runSuccessfulJob() throws Exception {
        runner.run();
        assertEquals(EXIT_WITHOUT_ERRORS, runner.getExitCode());
        Map<Long, ReleaseRecordEntity> releaseRecordsAfterStudy1Ingestion =
                this.mongoOperations.findAll(ReleaseRecordEntity.class).stream()
                                    .collect(Collectors.toMap(ReleaseRecordEntity::getAccession, entity -> entity));
        assertEquals(3, releaseRecordsAfterStudy1Ingestion.size());
        ReleaseRecordEntity indelReleaseRecord = releaseRecordsAfterStudy1Ingestion.get(3000000001L);
        assertEquals(5000000001L, indelReleaseRecord.getAssociatedSubmittedVariantEntities().get(0).getAccession());
        assertEquals(77L, indelReleaseRecord.getStart());
        assertEquals("CM001954.1", indelReleaseRecord.getContig());
        assertEquals("G", indelReleaseRecord.getAssociatedSubmittedVariantEntities().get(0).getReferenceAllele());
        assertEquals("T", indelReleaseRecord.getAssociatedSubmittedVariantEntities().get(0).getAlternateAllele());
    }
}
