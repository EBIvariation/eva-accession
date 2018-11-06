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
package uk.ac.ebi.eva.accession.dbsnp.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.processors.SubmittedVariantDeclusterProcessor;
import uk.ac.ebi.eva.accession.dbsnp.test.VariantAssertions;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;
import static uk.ac.ebi.eva.accession.dbsnp.io.DbsnpClusteredVariantDeclusteredWriter.DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.CLUSTERED_VARIANT_ACCESSION_1;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.CLUSTERED_VARIANT_ACCESSION_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.CLUSTERED_VARIANT_ACCESSION_3;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.PROJECT_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.START_1;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.START_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.SUBMITTED_VARIANT_ACCESSION_1;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.SUBMITTED_VARIANT_ACCESSION_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.SUBMITTED_VARIANT_ACCESSION_3;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.SUBMITTED_VARIANT_ACCESSION_4;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.TAXONOMY_1;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.TAXONOMY_2;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildClusteredVariant;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildClusteredVariantEntity;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildSimpleWrapper;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildSubmittedVariant;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.buildSubmittedVariantEntity;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.defaultClusteredVariant;
import static uk.ac.ebi.eva.accession.dbsnp.test.VariantBuilders.defaultSubmittedVariant;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, SubmittedVariantAccessioningConfiguration.class})
public class DbsnpVariantsWriterTest {

    private DbsnpVariantsWriter dbsnpVariantsWriter;

    private Function<IClusteredVariant, String> hashingFunctionClustered;

    @Autowired
    private MongoTemplate mongoTemplate;

    private ImportCounts importCounts;

    @Autowired
    private DbsnpSubmittedVariantOperationRepository operationRepository;

    @Autowired
    private DbsnpSubmittedVariantAccessioningRepository submittedVariantRepository;

    @Autowired
    private DbsnpClusteredVariantOperationRepository clusteredOperationRepository;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository clusteredVariantRepository;
    
    private VariantAssertions assertions;

    @Before
    public void setUp() throws Exception {
        importCounts = new ImportCounts();
        dbsnpVariantsWriter = new DbsnpVariantsWriter(mongoTemplate, operationRepository, submittedVariantRepository,
                                                      clusteredOperationRepository, clusteredVariantRepository,
                                                      importCounts);
        hashingFunctionClustered = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpSubmittedVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpSubmittedVariantOperationEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantOperationEntity.class);
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertions = new VariantAssertions(mongoTemplate, importCounts);
    }

    @Test
    public void writeBasicVariant() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertions.assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertClusteredVariantDeclusteredStored(0);
    }
    
    @Test
    public void writeComplexVariant() throws Exception {
        SubmittedVariant submittedVariant1 = defaultSubmittedVariant();
        SubmittedVariant submittedVariant2 = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                  "reference", "alternate_2",
                                                                  CLUSTERED_VARIANT_ACCESSION_1,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED, null);

        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 = buildSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, submittedVariant1);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity2 = buildSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, submittedVariant2);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(
                Arrays.asList(dbsnpSubmittedVariantEntity1, dbsnpSubmittedVariantEntity2));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));
        assertions.assertSubmittedVariantsStored(2, dbsnpSubmittedVariantEntity1, dbsnpSubmittedVariantEntity2);
        assertions.assertClusteredVariantStored(1, wrapper);
    }

    @Test
    public void declusterVariantWithMismatchingAlleles() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant = new SubmittedVariant("assembly", TAXONOMY_1, "project", "contig", START_1,
                                                                 "reference", "alternate",
                                                                 CLUSTERED_VARIANT_ACCESSION_1,
                                                                 DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                 allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity1 = buildSubmittedVariantEntity(
                SUBMITTED_VARIANT_ACCESSION_1, submittedVariant);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(dbsnpSubmittedVariantEntity1));
        wrapper.setOperations(decluster(dbsnpSubmittedVariantEntity1));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        assertions.assertSubmittedVariantDeclusteredStored(wrapper);
        assertions.assertClusteredVariantStored(0);
        assertions.assertDeclusteringHistoryStored(wrapper.getClusteredVariant().getAccession(),
                                        wrapper.getSubmittedVariants().get(0));
        assertions.assertClusteredVariantDeclusteredStored(1, wrapper);
    }
    
    @Test
    public void repeatedClusteredVariants() throws Exception {
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate",
                                                                  CLUSTERED_VARIANT_ACCESSION_1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertions.assertClusteredVariantStored(1, wrapper1);
    }

    @Test
    public void repeatedClusteredVariantsPartiallyDeclustered() throws Exception {
        SubmittedVariant submittedVariant1 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_1, PROJECT_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));
        wrapper2.setOperations(decluster(submittedVariantEntity2));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertions.assertClusteredVariantStored(1, wrapper1);
        assertions.assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    private List<DbsnpSubmittedVariantOperationEntity> decluster(DbsnpSubmittedVariantEntity dbsnpSubmittedVariantEntity) {
        DbsnpSubmittedVariantOperationEntity operation = new SubmittedVariantDeclusterProcessor().createOperation(
                dbsnpSubmittedVariantEntity, Collections.singletonList("Declustered"));
        dbsnpSubmittedVariantEntity.setClusteredVariantAccession(null);
        return Collections.singletonList(operation);
    }

    @Test
    public void mergedClusteredVariantsPartiallyDeclustered() throws Exception {
        SubmittedVariant submittedVariant1 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_1);
        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_1, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));
        wrapper1.setOperations(decluster(submittedVariantEntity1));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant3);
        ClusteredVariant clusteredVariant2 = defaultClusteredVariant();
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                         clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));
        wrapper2.setClusteredVariant(clusteredVariantEntity2);

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));

        assertions.assertClusteredVariantStored(1, wrapper1);
        assertions.assertClusteredVariantDeclusteredStored(1, wrapper1);
        assertions.assertClusteredVariantMergeOperationStored(1, 1, wrapper1.getClusteredVariant());
        assertEquals(CLUSTERED_VARIANT_ACCESSION_1, submittedVariantEntity3.getClusteredVariantAccession());
        assertions.assertSubmittedVariantsStored(2, submittedVariantEntity1, submittedVariantEntity3);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(3, 1, CLUSTERED_VARIANT_ACCESSION_1);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(3, 1, CLUSTERED_VARIANT_ACCESSION_2);
        assertions.assertSubmittedVariantMergeOperationStored(3, 1, submittedVariantEntity2);
    }

    @Test
    public void mergedClusteredVariantsDeclusteredAndNonDeclusteredSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant1 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));


        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2, PROJECT_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant2);
        SubmittedVariant submittedVariant3 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2, PROJECT_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant3);

        ClusteredVariant clusteredVariant2 = defaultClusteredVariant();
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                          clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setSubmittedVariants(Arrays.asList(submittedVariantEntity2, submittedVariantEntity3));
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setOperations(decluster(submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));

        assertions.assertClusteredVariantStored(1, wrapper1);
        assertions.assertClusteredVariantDeclusteredStored(1, wrapper2);
        assertions.assertClusteredVariantMergeOperationStored(1, 1, wrapper1.getClusteredVariant());
        assertEquals(CLUSTERED_VARIANT_ACCESSION_1, submittedVariantEntity2.getClusteredVariantAccession());
        assertions.assertSubmittedVariantsStored(3, submittedVariantEntity1, submittedVariantEntity2, submittedVariantEntity3);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(2, 0, CLUSTERED_VARIANT_ACCESSION_1);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(2, 2, CLUSTERED_VARIANT_ACCESSION_2);
        assertions.assertSubmittedVariantMergeOperationStored(2, 0, submittedVariantEntity2);
    }

    @Test
    public void repeatedClusteredVariantsCompletelyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity1));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        List<DbsnpSubmittedVariantOperationEntity> operationEntity1 = decluster(submittedVariantEntity1);
        wrapper1.setOperations(operationEntity1);
        wrapper2.setOperations(operationEntity1);

        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        assertions.assertClusteredVariantStored(0);
        assertions.assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void multiallelicClusteredVariantsPartiallyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);

        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);

        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));

        wrapper.setOperations(decluster(submittedVariantEntity1));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertClusteredVariantDeclusteredStored(1, wrapper);
    }

    @Test
    public void multiallelicClusteredVariantsCompletelyDeclustered() throws Exception {
        boolean allelesMatch = false;
        SubmittedVariant submittedVariant1 = new SubmittedVariant("assembly", TAXONOMY_2, "project", "contig", START_1,
                                                                  "reference", "alternate", null,
                                                                  DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                                  allelesMatch, DEFAULT_VALIDATED, null);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant1);

        DbsnpVariantsWrapper wrapper1 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity1, submittedVariantEntity2));

        wrapper1.setOperations(decluster(submittedVariantEntity1));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper1));
        assertions.assertClusteredVariantStored(0);
        assertions.assertClusteredVariantDeclusteredStored(1, wrapper1);
    }

    @Test
    public void mergeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // Try to write wrapper twice, the second time it will be considered a duplicate and ignored
        // wrapper2 will be merged into the previous accession
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertions.assertSubmittedVariantMergeOperationStored(1, 1, submittedVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity2));

        // wrapper3 should not issue another identical merge event
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2, wrapper3));

        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertions.assertSubmittedVariantMergeOperationStored(1, 1, submittedVariantEntity);
    }

    @Test
    public void mergeOnlyOnceDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity2, submittedVariantEntity2));

        // should not issue another identical merge event for the second variant in wrapper2
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertions.assertSubmittedVariantMergeOperationStored(1, 1, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity2));

        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_3,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper3 = buildSimpleWrapper(Arrays.asList(submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper, wrapper3));

        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertions.assertSubmittedVariantMergeOperationStored(2, 2, submittedVariantEntity);
    }

    @Test
    public void mergeThreeDuplicateSubmittedVariantsInTheSameWrapper() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpVariantsWrapper wrapper = buildSimpleWrapper(Collections.singletonList(submittedVariantEntity));

        dbsnpVariantsWriter.write(Collections.singletonList(wrapper));

        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_3,
                                                                                          submittedVariant);
        DbsnpVariantsWrapper wrapper2 = buildSimpleWrapper(
                Arrays.asList(submittedVariantEntity2, submittedVariantEntity3));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertions.assertSubmittedVariantMergeOperationStored(2, 2, submittedVariantEntity);
    }
    
    @Test
    public void mergeDuplicateClusteredVariantsInTheSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                                         clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper));

        assertions.assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertions.assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertions.assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    expectedSubmittedVariantEntity2);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(1, 1, clusteredVariantEntity2.getAccession());
        assertions.assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    private DbsnpSubmittedVariantEntity changeRS(DbsnpSubmittedVariantEntity submittedVariant, Long mergedInto) {
        // Need to create a new one because DbsnpSubmittedVariantEntity has no setters
        SubmittedVariant variant = new SubmittedVariant(submittedVariant);
        variant.setClusteredVariantAccession(mergedInto);

        Long accession = submittedVariant.getAccession();
        String hash = submittedVariant.getHashedMessage();
        int version = submittedVariant.getVersion();
        return new DbsnpSubmittedVariantEntity(accession, hash, variant, version);
    }

    @Test
    public void mergeDuplicateClusteredVariants() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                                         clusteredVariant);

        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));


        SubmittedVariant submittedVariant2 = defaultSubmittedVariant();
        submittedVariant2.setStart(START_2);
        submittedVariant2.setClusteredVariantAccession(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));
        dbsnpVariantsWriter.write(Arrays.asList(wrapper2));

        assertions.assertClusteredVariantStored(1, wrapper);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity.getAccession());
        assertions.assertSubmittedVariantsStored(2, submittedVariantEntity, expectedSubmittedVariantEntity2);
        assertions.assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    expectedSubmittedVariantEntity2);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(1, 1, clusteredVariantEntity2.getAccession());
        assertions.assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    @Test
    public void mergeThreeDuplicateClusteredVariantsInSameChunk() throws Exception {
        SubmittedVariant submittedVariant = defaultSubmittedVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                         submittedVariant);
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_1,
                                                                                         clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(CLUSTERED_VARIANT_ACCESSION_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_2,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        Long clusteredVariantAccession3 = CLUSTERED_VARIANT_ACCESSION_3;
        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(SUBMITTED_VARIANT_ACCESSION_1,
                                                                                          submittedVariant3);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(CLUSTERED_VARIANT_ACCESSION_3,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));


        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3));

        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertSubmittedVariantsStored(1, submittedVariantEntity);
        assertions.assertSubmittedVariantsHaveActiveClusteredVariantsAccession(wrapper.getClusteredVariant().getAccession(),
                                                                    wrapper.getSubmittedVariants().get(0),
                                                                    wrapper2.getSubmittedVariants().get(0),
                                                                    wrapper3.getSubmittedVariants().get(0));
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(2, 1, clusteredVariantEntity2.getAccession());
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(2, 1, clusteredVariantEntity3.getAccession());

        assertions.assertClusteredVariantMergeOperationStored(2, 2, clusteredVariantEntity);
        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    /**
     * The test data is not real, but this exact thing happened with rs638662487.
     * <p>
     * rs638662487 should be declustered from ss1387800177 because one orientation is unknown. This is tracked writing
     * the RS in dbsnpClusteredVariantEntityDeclustered instead of dbsnpClusteredVariantEntity.
     * <p>
     * However, another equivalent RS (rs268262202) was already declustered, and as this case is a collision of same
     * hash but different accession, it should be written in the clustered operations collection as a merge.
     */
    @Test
    public void declusterRs638662487() throws Exception {
        Long clusteredVariantAccession1 = 268262202L;
        Long clusteredVariantAccession2 = 638662487L;
        Long submittedVariantAccession1 = 528860089L;
        Long submittedVariantAccession2 = 1387800177L;

        SubmittedVariant submittedVariant1 = buildSubmittedVariant(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity declusteredSubmittedVariantEntity1 = buildSubmittedVariantEntity(
                submittedVariantAccession1, submittedVariant1);
        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                         clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity);
        wrapper.setOperations(decluster(declusteredSubmittedVariantEntity1));
        wrapper.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariantEntity1));

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2, PROJECT_2);
        DbsnpSubmittedVariantEntity declusteredSubmittedVariantEntity2 = buildSubmittedVariantEntity(
                submittedVariantAccession2, submittedVariant2);
        ClusteredVariant clusteredVariant2 = defaultClusteredVariant();
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant2);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setOperations(decluster(declusteredSubmittedVariantEntity2));
        wrapper2.setSubmittedVariants(Collections.singletonList(declusteredSubmittedVariantEntity2));

        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2));

        assertions.assertSubmittedVariantsStored(2, declusteredSubmittedVariantEntity1, declusteredSubmittedVariantEntity2);
        assertEquals(1, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
        assertions.assertClusteredVariantMergeOperationStored(1, 1, clusteredVariantEntity);
    }

    @Test
    public void mergeThreeClusteredVariantsAndTwoPairsOfSubmittedVariants() throws Exception {
        // given
        Long clusteredVariantAccession1 = 42568024L;
        Long clusteredVariantAccession2 = 42568025L;
        Long clusteredVariantAccession3 = 136611820L;
        Long submittedVariantAccession1 = 64289612L;
        Long submittedVariantAccession2 = 64289614L;
        Long submittedVariantAccession3 = 266911375L;
        Long submittedVariantAccession4 = 266602754L;

        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        SubmittedVariant submittedVariant1 = buildSubmittedVariant(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                          submittedVariant1);
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper = new DbsnpVariantsWrapper();
        wrapper.setClusteredVariant(clusteredVariantEntity1);
        wrapper.setSubmittedVariants(Collections.singletonList(submittedVariantEntity1));

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant);
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        SubmittedVariant submittedVariant4 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant3);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = buildSubmittedVariantEntity(submittedVariantAccession4,
                                                                                          submittedVariant4);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);

        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));

        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();
        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper, wrapper2, wrapper3, wrapper4));

        // then
        assertions.assertClusteredVariantStored(1, wrapper);
        assertions.assertClusteredVariantMergeOperationStored(2, 2, clusteredVariantEntity1);

        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = changeRS(submittedVariantEntity2,
                                                                               clusteredVariantEntity1.getAccession());
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3,
                                                                               clusteredVariantEntity1.getAccession());
        assertions.assertSubmittedVariantsStored(2, submittedVariantEntity1, expectedSubmittedVariantEntity3);
        assertions.assertSubmittedVariantsHaveActiveClusteredVariantsAccession(clusteredVariantAccession1,
                                                                               expectedSubmittedVariantEntity2,
                                                                               expectedSubmittedVariantEntity3);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(5, 1,
                                                                                        clusteredVariantAccession2);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(5, 2,
                                                                                        clusteredVariantAccession3);
        assertions.assertSubmittedVariantMergeOperationStored(5, 1, submittedVariantEntity1);
        assertions.assertSubmittedVariantMergeOperationStored(5, 1, submittedVariantEntity3);

        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));
    }

    /**
     * This test checks that a clustered variant can be merged two times: one merge after making hash collision in the
     * main clustered variant collection, and the other merge after making hash collision in the collection for
     * declustered clustered variants.
     *
     * This scenario happened with rs136611820 (after storing rs42568024 and rs42568025). The actual data is more
     * complex, but for the scope of this test, the rest of the complexity is irrelevant.
     */
    @Test
    public void rs136611820() throws Exception {
        // given
        DbsnpVariantsWrapper wrapper1 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();

        setupRs136611820(wrapper1, wrapper2, wrapper3, wrapper4);

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2, wrapper3, wrapper4));

        // then
        assertRs136611820(wrapper1, wrapper2, wrapper3, wrapper4);
    }

    @Test
    public void rs136611820MultiChunk() throws Exception {
        // given
        DbsnpVariantsWrapper wrapper1 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();

        setupRs136611820(wrapper1, wrapper2, wrapper3, wrapper4);

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper3));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper4));

        // then
        assertRs136611820(wrapper1, wrapper2, wrapper3, wrapper4);
    }

    public void setupRs136611820(DbsnpVariantsWrapper wrapper1, DbsnpVariantsWrapper wrapper2,
                                 DbsnpVariantsWrapper wrapper3, DbsnpVariantsWrapper wrapper4) {
        Long clusteredVariantAccession1 = 42568024L;
        Long clusteredVariantAccession2 = 42568025L;
        Long clusteredVariantAccession3 = 136611820L;
        Long submittedVariantAccession1 = 64289612L;
        Long submittedVariantAccession2 = 64289614L;
        Long submittedVariantAccession3 = 266911375L;
        Long submittedVariantAccession4 = 266911374L;

        ClusteredVariant clusteredVariant = defaultClusteredVariant();

        SubmittedVariant submittedVariant = buildSubmittedVariant(clusteredVariantAccession1, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                         submittedVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                         clusteredVariant);

        wrapper1.setClusteredVariant(clusteredVariantEntity);
        wrapper1.setSubmittedVariants(Collections.singletonList(submittedVariantEntity));
        wrapper1.setOperations(decluster(submittedVariantEntity));

        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant);
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));

        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        SubmittedVariant submittedVariant4 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant3);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = buildSubmittedVariantEntity(submittedVariantAccession4,
                                                                                          submittedVariant4);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant);

        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));
        wrapper3.setOperations(decluster(submittedVariantEntity3));

        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));
    }

    public void assertRs136611820(DbsnpVariantsWrapper wrapper1, DbsnpVariantsWrapper wrapper2,
                                  DbsnpVariantsWrapper wrapper3, DbsnpVariantsWrapper wrapper4) {
        Long clusteredVariantAccession1 = wrapper1.getClusteredVariant().getAccession();
        Long clusteredVariantAccession2 = wrapper2.getClusteredVariant().getAccession();
        Long clusteredVariantAccession3 = wrapper3.getClusteredVariant().getAccession();
        DbsnpSubmittedVariantEntity submittedVariantEntity = wrapper1.getSubmittedVariants().get(0);
        DbsnpClusteredVariantEntity clusteredVariantEntity = wrapper1.getClusteredVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = wrapper2.getSubmittedVariants().get(0);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = wrapper2.getClusteredVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = wrapper3.getSubmittedVariants().get(0);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = wrapper4.getSubmittedVariants().get(0);

        assertions.assertClusteredVariantStored(1, wrapper1);
        // the first non-declustered RS is clusteredVariantAccession2. The non-declustered clusteredVariantAccession3
        // (for submittedVariantEntity3) is merged into it. Then the declustered clusteredVariantAccession3 (for
        // submittedVariantEntity4) is merged into the previously declustered clusteredVariantAccession1. Then, we have
        // clusteredVariantEntity4 merged into 2 RS: clusteredVariantEntity and clusteredVariantEntity2.
        assertions.assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity2);
        assertions.assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity);

        List<DbsnpClusteredVariantEntity> declustered = mongoTemplate.findAll(
                DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
        assertEquals(1, declustered.size());
        assertEquals(clusteredVariantAccession1, declustered.get(0).getAccession());

        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity1 = changeRS(submittedVariantEntity, null);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity2 = submittedVariantEntity2;
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3, null);

        // as explained above, clusteredVariantAccession3 (for submittedVariantEntity4) was merged into
        // clusteredVariantAccession1 and clusteredVariantAccession2, but we update the rs field to the
        // clusteredVariantAccession2 because it's the active one.
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity4 = changeRS(submittedVariantEntity4,
                                                                               clusteredVariantAccession2);

        assertions.assertSubmittedVariantsStored(4, expectedSubmittedVariantEntity1, expectedSubmittedVariantEntity2,
                                      expectedSubmittedVariantEntity3, expectedSubmittedVariantEntity4);
        assertions.assertSubmittedVariantsHaveActiveClusteredVariantsAccession(clusteredVariantAccession2,
                                                                               expectedSubmittedVariantEntity2,
                                                                               expectedSubmittedVariantEntity4);
        assertions.assertSubmittedOperationType(EventType.UPDATED, 3L);
        assertions.assertSubmittedOperationType(EventType.MERGED, 0L);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(3, 1,
                                                                                        clusteredVariantAccession1);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(3, 2,
                                                                                        clusteredVariantAccession3);
    }

    /**
     * This test shows how the current code allows a given RS to be merged into several other active RSs at the same
     * time.
     *
     * This can happen if there are several entries with different hashes but same clustered variant accession (for
     * instance, several locations, or several types), and 2 or more of those different hashes make collision with
     * other active RSs.
     *
     * The test data is not real, but this exact thing happened with rs193927678.
     *
     * rs193927678 is mapped in 2 positions, so corresponds to 2 clustered variants with different hash and same RS
     * accession. Each entry of this RS makes hash collision with rs1095750933 and rs347458720.
     *
     * So to decide which active RS should we link in each submitted variant, we have to take into account the hash as
     * well. One submitted variant will be linked to rs1095750933 and the other to rs347458720.
     *
     * Note that there will be 2 clustered variant merge operations, as there are 2 hashes for rs193927678. Each
     * operation is a merge into rs1095750933 and rs347458720. Moreover, there are no declustered operations
     * (either for submitted or for clustered variants).
     */
    @Test
    public void clusteredVariantMergedIntoSeveralActiveClusteredVariants() throws Exception {
        // given
        DbsnpVariantsWrapper wrapper1 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();

        setupClusteredVariantMergedIntoSeveralActiveClusteredVariants(wrapper1, wrapper2, wrapper3, wrapper4);

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2, wrapper3, wrapper4));

        // then
        assertClusteredVariantMergedIntoSeveralActiveClusteredVariants(wrapper1, wrapper2, wrapper3, wrapper4);
    }

    @Test
    public void clusteredVariantMergedIntoSeveralActiveClusteredVariantsMultiChunk() throws Exception {
        // given
        DbsnpVariantsWrapper wrapper1 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper2 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper3 = new DbsnpVariantsWrapper();
        DbsnpVariantsWrapper wrapper4 = new DbsnpVariantsWrapper();

        setupClusteredVariantMergedIntoSeveralActiveClusteredVariants(wrapper1, wrapper2, wrapper3, wrapper4);

        // when
        dbsnpVariantsWriter.write(Arrays.asList(wrapper1, wrapper2));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper3));
        dbsnpVariantsWriter.write(Collections.singletonList(wrapper4));

        // then
        assertClusteredVariantMergedIntoSeveralActiveClusteredVariants(wrapper1, wrapper2, wrapper3, wrapper4);
    }

    public void setupClusteredVariantMergedIntoSeveralActiveClusteredVariants(DbsnpVariantsWrapper wrapper1,
                                                                              DbsnpVariantsWrapper wrapper2,
                                                                              DbsnpVariantsWrapper wrapper3,
                                                                              DbsnpVariantsWrapper wrapper4) {
        Long clusteredVariantAccession1 = 347458720L;
        Long clusteredVariantAccession2 = 1095750933L;
        Long clusteredVariantAccession3 = 193927678L;
        Long submittedVariantAccession1 = 2688593462L;
        Long submittedVariantAccession2 = 2688600186L;
        Long submittedVariantAccession3 = 252447620L;
        ClusteredVariant clusteredVariant1 = defaultClusteredVariant();

        SubmittedVariant submittedVariant1 = buildSubmittedVariant(clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = buildSubmittedVariantEntity(submittedVariantAccession1,
                                                                                          submittedVariant1);
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = buildClusteredVariantEntity(clusteredVariantAccession1,
                                                                                          clusteredVariant1);

        wrapper1.setClusteredVariant(clusteredVariantEntity1);
        wrapper1.setSubmittedVariants(Collections.singletonList(submittedVariantEntity1));


        ClusteredVariant clusteredVariant2 = buildClusteredVariant(START_2);
        SubmittedVariant submittedVariant2 = buildSubmittedVariant(clusteredVariantAccession2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = buildSubmittedVariantEntity(submittedVariantAccession2,
                                                                                          submittedVariant2);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = buildClusteredVariantEntity(clusteredVariantAccession2,
                                                                                          clusteredVariant2);
        wrapper2.setClusteredVariant(clusteredVariantEntity2);
        wrapper2.setSubmittedVariants(Collections.singletonList(submittedVariantEntity2));


        SubmittedVariant submittedVariant3 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2);
        SubmittedVariant submittedVariant4 = buildSubmittedVariant(clusteredVariantAccession3, PROJECT_2, START_2);
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant3);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = buildSubmittedVariantEntity(submittedVariantAccession3,
                                                                                          submittedVariant4);
        DbsnpClusteredVariantEntity clusteredVariantEntity3 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant1);
        DbsnpClusteredVariantEntity clusteredVariantEntity4 = buildClusteredVariantEntity(clusteredVariantAccession3,
                                                                                          clusteredVariant2);

        wrapper3.setClusteredVariant(clusteredVariantEntity3);
        wrapper3.setSubmittedVariants(Collections.singletonList(submittedVariantEntity3));

        wrapper4.setClusteredVariant(clusteredVariantEntity4);
        wrapper4.setSubmittedVariants(Collections.singletonList(submittedVariantEntity4));
    }

    public void assertClusteredVariantMergedIntoSeveralActiveClusteredVariants(DbsnpVariantsWrapper wrapper1,
                                                                               DbsnpVariantsWrapper wrapper2,
                                                                               DbsnpVariantsWrapper wrapper3,
                                                                               DbsnpVariantsWrapper wrapper4) {
        Long clusteredVariantAccession1 = wrapper1.getClusteredVariant().getAccession();
        Long clusteredVariantAccession2 = wrapper2.getClusteredVariant().getAccession();
        Long clusteredVariantAccession3 = wrapper3.getClusteredVariant().getAccession();
        DbsnpSubmittedVariantEntity submittedVariantEntity1 = wrapper1.getSubmittedVariants().get(0);
        DbsnpClusteredVariantEntity clusteredVariantEntity1 = wrapper1.getClusteredVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity2 = wrapper2.getSubmittedVariants().get(0);
        DbsnpClusteredVariantEntity clusteredVariantEntity2 = wrapper2.getClusteredVariant();
        DbsnpSubmittedVariantEntity submittedVariantEntity3 = wrapper3.getSubmittedVariants().get(0);
        DbsnpSubmittedVariantEntity submittedVariantEntity4 = wrapper4.getSubmittedVariants().get(0);

        assertions.assertClusteredVariantStored(2, wrapper1, wrapper2);
        assertions.assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity1);
        assertions.assertClusteredVariantMergeOperationStored(2, 1, clusteredVariantEntity2);

        assertEquals(0, mongoTemplate.count(new Query(), DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME));

        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity3 = changeRS(submittedVariantEntity3,
                                                                               clusteredVariantAccession1);
        DbsnpSubmittedVariantEntity expectedSubmittedVariantEntity4 = changeRS(submittedVariantEntity4,
                                                                               clusteredVariantAccession2);

        assertions.assertSubmittedVariantsStored(4, submittedVariantEntity1, submittedVariantEntity2,
                                      expectedSubmittedVariantEntity3, expectedSubmittedVariantEntity4);
        assertions.assertSubmittedVariantsHaveActiveClusteredVariantsAccession(clusteredVariantAccession1,
                                                                               submittedVariantEntity1,
                                                                               expectedSubmittedVariantEntity3);
        assertions.assertSubmittedVariantsHaveActiveClusteredVariantsAccession(clusteredVariantAccession2,
                                                                               submittedVariantEntity2,
                                                                               expectedSubmittedVariantEntity4);
        assertions.assertSubmittedOperationType(EventType.UPDATED, 2L);
        assertions.assertSubmittedOperationType(EventType.MERGED, 0L);
        assertions.assertSubmittedVariantsUpdateOperationsHaveClusteredVariantAccession(2, 2,
                                                                                        clusteredVariantAccession3);
    }
}
