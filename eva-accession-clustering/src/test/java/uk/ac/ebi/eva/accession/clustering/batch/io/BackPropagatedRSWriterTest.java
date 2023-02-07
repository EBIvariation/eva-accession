/*
 * Copyright 2023 EMBL - European Bioinformatics Institute
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
import org.junit.*;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.clustering.test.configuration.BatchTestConfiguration;
import uk.ac.ebi.eva.accession.clustering.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.*;
import uk.ac.ebi.eva.accession.core.model.eva.*;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.*;
import static uk.ac.ebi.eva.accession.clustering.configuration.BeanNames.*;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes={BatchTestConfiguration.class})
@TestPropertySource("classpath:backpropagation-test.properties")
public class BackPropagatedRSWriterTest {

    private static final String TEST_DB = "test-db";

    private static final int TAXONOMY = 60711;

    private static final String ASM1 = "asm1";

    private static final String ASM2 = "asm2";

    private static final String ASM3 = "asm3";

    private static final String ASM4 = "asm4";

    private static final String PROJECT = "PRJ1";

    @Autowired
    private MetricCompute metricCompute;

    @Autowired
    private MongoTemplate mongoTemplate;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Autowired
    private SubmittedVariantAccessioningService submittedVariantAccessioningService;

    @Autowired
    @Qualifier(CLUSTERED_CLUSTERING_WRITER)
    private ClusteringWriter clusteringWriter;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private static class RSLocus {
        String assembly;
        String contig;
        long start;
        VariantType type;

        public RSLocus(String assembly, String contig, long start, VariantType type) {
            this.assembly = assembly;
            this.contig = contig;
            this.start = start;
            this.type = type;
        }

        public String getHash() {
            Function<IClusteredVariant, String> hashingFunction =
                    new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
            return hashingFunction.apply(
                    new ClusteredVariant(this.assembly, -1, this.contig, this.start, this.type, null,
                            null));
        }
    }

    @Before
    public void setUp() throws Exception {
        mongoTemplate.getDb().drop();
    }

    @After
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    @DirtiesContext
    /*
     * See https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=810976681
     */
    public void runBackPropagationWithMultipleAssemblies() throws AccessionCouldNotBeGeneratedException {
        RSLocus rsLocus1 = new RSLocus(ASM1, "chr1", 100L, VariantType.SNV);
        RSLocus rsLocus2 = new RSLocus(ASM2, "chr1", 101L, VariantType.SNV);
        RSLocus rsLocus3 = new RSLocus(ASM2, "chr1", 102L, VariantType.SNV);

        ClusteredVariantEntity rs1 = createRS(1L, rsLocus1);
        ClusteredVariantEntity rs2 = createRS(2L, rsLocus2);
        SubmittedVariantEntity ss1 = createSS(1L, 1L, rsLocus1, "A", "T", false);
        SubmittedVariantEntity ss2 = createSS(2L, null, rsLocus1, "A", "C", false);
        SubmittedVariantEntity ss1_asm2_remap = createSS(ss1.getAccession(), 1L, rsLocus2, "C", "T", true);
        SubmittedVariantEntity ss2_asm2_remap = createSS(ss2.getAccession(), 2L, rsLocus3, "C", "A", true);

        this.mongoTemplate.insert(Arrays.asList(rs1, rs2), DbsnpClusteredVariantEntity.class);
        this.mongoTemplate.insert(Arrays.asList(ss1, ss2, ss1_asm2_remap, ss2_asm2_remap),
                DbsnpSubmittedVariantEntity.class);

        BackPropagatedRSWriter backPropagatedRSWriter = new BackPropagatedRSWriter(ASM2, this.clusteringWriter,
                this.submittedVariantAccessioningService, this.mongoTemplate, this.metricCompute);
        backPropagatedRSWriter.write(Arrays.asList(ss2));
        // As of T2, see https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=688023832
        assertSSBackPropRSAssociation(ss2.getAccession(), rs2.getAccession(), ASM1);

        RSLocus rsLocus4 = new RSLocus(ASM3, "chr1", 102L, VariantType.SNV);
        SubmittedVariantEntity ss1_asm3_remap = createSS(ss1.getAccession(), 1L, rsLocus4, "G", "T", true);
        SubmittedVariantEntity ss2_asm3_remap = createSS(ss2.getAccession(), 1L, rsLocus4, "G", "A", true);
        this.mongoTemplate.insert(Arrays.asList(ss1_asm3_remap, ss2_asm3_remap), DbsnpSubmittedVariantEntity.class);
        backPropagatedRSWriter = new BackPropagatedRSWriter(ASM3, this.clusteringWriter,
                this.submittedVariantAccessioningService, this.mongoTemplate, this.metricCompute);
        backPropagatedRSWriter.write(Arrays.asList(ss2));
        // As of T3, see https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=490224900
        assertSSBackPropRSAssociation(ss2.getAccession(), rs1.getAccession(), ASM1);

        RSLocus rsLocus5 = new RSLocus(ASM4, "chr1", 107L, VariantType.SNV);
        RSLocus rsLocus6 = new RSLocus(ASM4, "chr1", 108L, VariantType.SNV);
        ClusteredVariantEntity rs3 = createRS(3L, rsLocus6);
        SubmittedVariantEntity ss1_asm4_remap = createSS(ss1.getAccession(), 1L, rsLocus5, "C", "T", true);
        SubmittedVariantEntity ss2_asm4_remap = createSS(ss2.getAccession(), 3L, rsLocus6, "C", "A", true);
        this.mongoTemplate.insert(Arrays.asList(rs3), DbsnpClusteredVariantEntity.class);
        this.mongoTemplate.insert(Arrays.asList(ss1_asm4_remap, ss2_asm4_remap), DbsnpSubmittedVariantEntity.class);
        backPropagatedRSWriter = new BackPropagatedRSWriter(ASM4, this.clusteringWriter,
                this.submittedVariantAccessioningService, this.mongoTemplate, this.metricCompute);
        backPropagatedRSWriter.write(Arrays.asList(ss2));
        // As of T4, see https://docs.google.com/spreadsheets/d/1KQLVCUy-vqXKgkCDt2czX6kuMfsjfCc9uBsS19MZ6dY/edit#rangeid=48828480
        assertSSBackPropRSAssociation(ss2.getAccession(), rs3.getAccession(), ASM1);
    }

    private void assertSSBackPropRSAssociation(Long ssID, Long expectedBackPropRS, String originalAssembly) {
        List<ISubmittedVariant> ssInOriginalAssembly =
                this.submittedVariantAccessioningService
                        .getAllActiveByAssemblyAndAccessionIn(originalAssembly, Collections.singletonList(ssID))
                        .stream().map(AccessionWrapper::getData).collect(Collectors.toList());
        assertEquals(1, ssInOriginalAssembly.size());
        Long actualBackPropRS = ssInOriginalAssembly.get(0).getBackPropagatedVariantAccession();
        if (Objects.isNull(expectedBackPropRS)) {
            assertNull(actualBackPropRS);
        } else {
            assertEquals(expectedBackPropRS, actualBackPropRS);
        }
    }

    private SubmittedVariantEntity createSS(Long ssAccession, Long rsAccession, RSLocus rsLocus, String reference,
                                            String alternate, boolean remappedFromAnotherAssembly) {
        Function<ISubmittedVariant, String> hashingFunction =  new SubmittedVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        SubmittedVariant submittedVariant = new SubmittedVariant(rsLocus.assembly, TAXONOMY, PROJECT, rsLocus.contig,
                rsLocus.start, reference, alternate, rsAccession);
        String hash = hashingFunction.apply(submittedVariant);
        SubmittedVariantEntity submittedVariantEntity = new SubmittedVariantEntity(ssAccession, hash, submittedVariant,
                1);
        if (remappedFromAnotherAssembly) {
            submittedVariantEntity.setRemappedFrom(ASM1);
        }
        return submittedVariantEntity;
    }

    private ClusteredVariantEntity createRS(Long rsAccession, RSLocus rsLocus) {
        Function<IClusteredVariant, String> hashingFunction =  new ClusteredVariantSummaryFunction().andThen(
                new SHA1HashingFunction());
        ClusteredVariant clusteredVariant = new ClusteredVariant(rsLocus.assembly, TAXONOMY, rsLocus.contig,
                rsLocus.start, rsLocus.type, false, null);
        String hash = hashingFunction.apply(clusteredVariant);
        return new ClusteredVariantEntity(rsAccession, hash, clusteredVariant);
    }
}
