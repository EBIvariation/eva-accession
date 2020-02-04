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
package uk.ac.ebi.eva.accession.dbsnp2.batch.io;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.test.StepScopeTestExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.batch.io.DbsnpClusteredVariantOperationWriter;
import uk.ac.ebi.eva.accession.core.batch.io.DbsnpClusteredVariantWriter;
import uk.ac.ebi.eva.accession.core.batch.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantAccessioningRepository;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.repository.nonhuman.dbsnp.DbsnpClusteredVariantOperationRepository;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp2.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestExecutionListeners({DependencyInjectionTestExecutionListener.class,
        StepScopeTestExecutionListener.class})
@TestPropertySource({"classpath:application.properties"})
public class DbsnpJsonClusteredVariantsWriterTest {

    private DbsnpJsonClusteredVariantsWriter dbsnpJsonClusteredVariantsWriter;

    @Autowired
    private DbsnpClusteredVariantAccessioningRepository dbsnpClusteredVariantAccessioningRepository;

    @Autowired
    private DbsnpClusteredVariantOperationRepository dbsnpClusteredVariantOperationRepository;

    @Autowired
    private MongoTemplate mongoTemplate;

    private ImportCounts importCounts;

    private DbsnpClusteredVariantEntity variantEntity1;
    private DbsnpClusteredVariantEntity variantEntity2;

    private Function<IClusteredVariant, String> hashingFuncClustered =
        new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

    @Before
    public void setUp() {
        importCounts = new ImportCounts();
        DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate,
                                                                                                  importCounts);
        DbsnpClusteredVariantOperationWriter dbsnpClusteredVariantOperationWriter =
                new DbsnpClusteredVariantOperationWriter(mongoTemplate, importCounts);
        variantEntity1 = buildClusteredVariantEntity(1L,
                                                     buildClusteredVariant("acsn1",
                                                                           "contig1",
                                                                           1L,
                                                                           VariantType.SNV));
        dbsnpJsonClusteredVariantsWriter =
                new DbsnpJsonClusteredVariantsWriter(dbsnpClusteredVariantWriter, dbsnpClusteredVariantOperationWriter,
                                                     dbsnpClusteredVariantOperationRepository,
                                                     dbsnpClusteredVariantAccessioningRepository);
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
        mongoTemplate.dropCollection(DbsnpClusteredVariantOperationEntity.class);
    }

    @Test
    public void writeSingleVariant() throws Exception {
        dbsnpJsonClusteredVariantsWriter.write(Collections.singletonList(variantEntity1));
        assertClusteredVariantStored(1, Collections.singletonList(variantEntity1));
    }

    @Test
    public void writeMultipleVariantsWithDifferentContig() throws Exception {
        variantEntity2 = buildClusteredVariantEntity(variantEntity1.getAccession(),
                                                     buildClusteredVariant(variantEntity1.getAssemblyAccession(),
                                                                           "contig2",
                                                                           variantEntity1.getStart(),
                                                                           variantEntity1.getType()));
        List<DbsnpClusteredVariantEntity> clusteredVariantEntities = Arrays.asList(variantEntity1, variantEntity2);
        dbsnpJsonClusteredVariantsWriter.write(clusteredVariantEntities);
        assertClusteredVariantStored(2, clusteredVariantEntities);
    }

    @Test
    public void writeMultipleVariantsWithDifferentStart() throws Exception {
        variantEntity2 = buildClusteredVariantEntity(variantEntity1.getAccession(),
                                                     buildClusteredVariant(variantEntity1.getAssemblyAccession(),
                                                                           variantEntity1.getContig(),
                                                                           2L,
                                                                           variantEntity1.getType()));
        List<DbsnpClusteredVariantEntity> clusteredVariantEntities = Arrays.asList(variantEntity1, variantEntity2);
        dbsnpJsonClusteredVariantsWriter.write(clusteredVariantEntities);
        assertClusteredVariantStored(2, clusteredVariantEntities);
    }

    @Test
    public void writeMultipleVariantsWithDifferentVariantType() throws Exception {
        variantEntity2 = buildClusteredVariantEntity(variantEntity1.getAccession(),
                                                     buildClusteredVariant(variantEntity1.getAssemblyAccession(),
                                                                           variantEntity1.getContig(),
                                                                           variantEntity1.getStart(),
                                                                           VariantType.DEL));
        List<DbsnpClusteredVariantEntity> clusteredVariantEntities = Arrays.asList(variantEntity1, variantEntity2);
        dbsnpJsonClusteredVariantsWriter.write(clusteredVariantEntities);
        assertClusteredVariantStored(2, clusteredVariantEntities);
    }

    @Test
    public void writeMultipleVariantsWithDifferentAssemblyAccession() throws Exception {
        variantEntity2 = buildClusteredVariantEntity(variantEntity1.getAccession(),
                                                     buildClusteredVariant("acsn2",
                                                                           variantEntity1.getContig(),
                                                                           variantEntity1.getStart(),
                                                                           variantEntity1.getType()));
        List<DbsnpClusteredVariantEntity> clusteredVariantEntities = Arrays.asList(variantEntity1, variantEntity2);
        dbsnpJsonClusteredVariantsWriter.write(clusteredVariantEntities);
        assertClusteredVariantStored(2, clusteredVariantEntities);
    }

    @Test
    public void writeMultipleVariantsWithDifferentRsIDWithSameClusteredVariant() throws Exception {
        variantEntity2 = buildClusteredVariantEntity(2L,
                                                     variantEntity1.getModel());
        DbsnpClusteredVariantEntity variantEntity3 = buildClusteredVariantEntity(3L,
                                                                                 variantEntity1.getModel());
        List<DbsnpClusteredVariantEntity> clusteredVariantEntities = Arrays.asList(variantEntity1, variantEntity2,
                                                                                   variantEntity3);
        dbsnpJsonClusteredVariantsWriter.write(clusteredVariantEntities);
        assertClusteredVariantStored(1, clusteredVariantEntities);

        DbsnpClusteredVariantOperationEntity clusteredVariantOperationEntity1 =
                new DbsnpClusteredVariantOperationEntity();
        DbsnpClusteredVariantOperationEntity clusteredVariantOperationEntity2 =
                new DbsnpClusteredVariantOperationEntity();
        clusteredVariantOperationEntity1.fill(EventType.MERGED, variantEntity2.getAccession(),
                                             variantEntity1.getAccession(),
                                             "Identical clustered variant received multiple RS identifiers",
                                             Arrays.asList(new DbsnpClusteredVariantInactiveEntity(variantEntity2)));
        clusteredVariantOperationEntity2.fill(EventType.MERGED, variantEntity3.getAccession(),
                                              variantEntity1.getAccession(),
                                              "Identical clustered variant received multiple RS identifiers",
                                              Arrays.asList(new DbsnpClusteredVariantInactiveEntity(variantEntity3)));
        assertClusteredVariantOperationStored(2, Arrays.asList(clusteredVariantOperationEntity1,
                                                               clusteredVariantOperationEntity2));
    }

    @Test
    public void writeDuplicateVariantShouldNotBeStored() throws Exception {
        List<DbsnpClusteredVariantEntity> clusteredVariantEntities = Arrays.asList(variantEntity1, variantEntity1);
        dbsnpJsonClusteredVariantsWriter.write(clusteredVariantEntities);
        assertClusteredVariantStored(1, clusteredVariantEntities);
    }

    private DbsnpClusteredVariantEntity buildClusteredVariantEntity(Long accession,
                                                                    IClusteredVariant clusteredVariant) {
        return new DbsnpClusteredVariantEntity(accession, hashingFuncClustered.apply(clusteredVariant), clusteredVariant);
    }

    private IClusteredVariant buildClusteredVariant(String accession, String contig, long start, VariantType variantType) {
        return new ClusteredVariant(accession,
                                    9606,
                                    contig,
                                    start,
                                    variantType,
                                    Boolean.FALSE,
                                    LocalDateTime.now());
    }

    private void assertClusteredVariantStored(int expectedVariants,
                                              List<DbsnpClusteredVariantEntity> clusteredVariantEntities) {
        List<DbsnpClusteredVariantEntity> actualClusteredVariantEntities = mongoTemplate.find
            (new Query(), DbsnpClusteredVariantEntity.class);
        assertEquals(expectedVariants, actualClusteredVariantEntities.size());
        assertEquals(expectedVariants, importCounts.getClusteredVariantsWritten());
        clusteredVariantEntities.forEach(entity ->
            assertTrue(actualClusteredVariantEntities.contains(entity)));
    }

    private void assertClusteredVariantOperationStored
            (int expectedVariants, List<DbsnpClusteredVariantOperationEntity> clusteredVariantOperationEntities) {
        List<DbsnpClusteredVariantOperationEntity> actualClusteredVariantOperationEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantOperationEntity.class);
        assertEquals(expectedVariants, actualClusteredVariantOperationEntities.size());
        assertEquals(expectedVariants, importCounts.getOperationsWritten());
        clusteredVariantOperationEntities.forEach(
                entity -> assertTrue(actualClusteredVariantOperationEntities.stream().filter(
                        actual -> dbsnpClusteredVariantOperationEntitiesEqual(actual, entity)).count() >= 1));
    }

    private boolean dbsnpClusteredVariantOperationEntitiesEqual(DbsnpClusteredVariantOperationEntity entity1,
                                                             DbsnpClusteredVariantOperationEntity entity2) {
        return entity1.getAccession().equals(entity2.getAccession()) &&
                entity1.getMergedInto().equals(entity2.getMergedInto()) &&
                entity1.getInactiveObjects().containsAll(entity2.getInactiveObjects()) &&
                entity1.getInactiveObjects().size() == entity2.getInactiveObjects().size();
    }
}
