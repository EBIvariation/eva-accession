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
package uk.ac.ebi.eva.accession.core.batch.io;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.item.Chunk;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.BulkOperationException;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.core.batch.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static uk.ac.ebi.eva.commons.core.models.VariantType.INDEL;
import static uk.ac.ebi.eva.commons.core.models.VariantType.SNV;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoTestConfiguration.class})
public class DbsnpClusteredVariantWriterTest extends MongoTestContainerHelper {

    private static final int TAXONOMY_1 = 3880;

    private static final Long ACCESSION_1 = 10000000001L;

    private static final Long ACCESSION_2 = 10000000002L;

    private static final Long ACCESSION_3 = 10000000003L;

    private static final int START = 100;

    private static final String ASSEMBLY = "assembly";

    private static final String CONTIG = "contig";

    private static final LocalDateTime CREATED_DATE = LocalDateTime.of(2018, Month.SEPTEMBER, 18, 9, 0);

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    @Autowired
    private MongoTemplate mongoTemplate;

    private Function<IClusteredVariant, String> hashingFunction;

    private ImportCounts importCounts;

    private ClusteredVariant clusteredVariant1;

    private ClusteredVariant clusteredVariant2;

    private DbsnpClusteredVariantEntity variantEntity1;

    private DbsnpClusteredVariantEntity variantEntity2;

    private DbsnpClusteredVariantEntity variantEntity3;

    private DbsnpClusteredVariantEntity duplicateVariantEntity1;

    private ClusteredVariant clusteredVariant3;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        importCounts = new ImportCounts();
        dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate, importCounts);
        hashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);

        // variants and entity objects
        clusteredVariant1 = new ClusteredVariant(ASSEMBLY, TAXONOMY_1, CONTIG, START, SNV, true, CREATED_DATE);
        ClusteredVariant duplicateClusteredVariant1 = new ClusteredVariant(ASSEMBLY, TAXONOMY_1, CONTIG, START, SNV,
                false, null);
        clusteredVariant2 = new ClusteredVariant(ASSEMBLY, TAXONOMY_1, CONTIG, START + 1, SNV, true, null);
        clusteredVariant3 = new ClusteredVariant(ASSEMBLY, TAXONOMY_1, CONTIG, START, INDEL, true, null);
        variantEntity1 = buildClusteredVariantEntity(ACCESSION_1, clusteredVariant1);
        duplicateVariantEntity1 = buildClusteredVariantEntity(ACCESSION_1, duplicateClusteredVariant1);
        variantEntity2 = buildClusteredVariantEntity(ACCESSION_2, clusteredVariant2);
        variantEntity3 = buildClusteredVariantEntity(ACCESSION_3, clusteredVariant3);
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    private DbsnpClusteredVariantEntity buildClusteredVariantEntity(long accession, ClusteredVariant clusteredVariant) {
        return new DbsnpClusteredVariantEntity(accession, hashingFunction.apply(clusteredVariant), clusteredVariant);
    }

    @Test
    public void saveSingleAccession() {
        dbsnpClusteredVariantWriter.write(Chunk.of(variantEntity1));
        assertJustOneVariantHasBeenStored();
    }

    private void assertJustOneVariantHasBeenStored() {
        List<DbsnpClusteredVariantEntity> storedVariants = mongoTemplate.find(new Query(),
                DbsnpClusteredVariantEntity.class);
        assertEquals(1, storedVariants.size());
        assertTrue(storedVariants.contains(variantEntity1));
        assertEquals(ACCESSION_1, storedVariants.get(0).getAccession());
        assertEquals(clusteredVariant1, storedVariants.get(0).getModel());
        assertEquals(1, importCounts.getClusteredVariantsWritten());
    }

    @Test
    public void exceptionThrownOnDuplicateIdenticalVariant() {
        BulkOperationException exception = assertThrows(BulkOperationException.class, () ->
                dbsnpClusteredVariantWriter.write(Chunk.of(variantEntity1, variantEntity1)));
        assertTrue(exception.getMessage().contains("duplicate key error"));
    }

    @Test
    public void duplicateIdenticalVariantIsStoredJustOnce() {
        try {
            dbsnpClusteredVariantWriter.write(Chunk.of(variantEntity1, variantEntity1));
            fail();
        } catch (Exception e) {
            // it's correct and expected that an exception is thrown here
        }
        assertJustOneVariantHasBeenStored();
    }


    @Test
    public void duplicateNotIdenticalVariantIsStoredJustOnce() {
        try {
            dbsnpClusteredVariantWriter.write(Chunk.of(variantEntity1, duplicateVariantEntity1));
            fail();
        } catch (Exception e) {
            // it's correct and expected that an exception is thrown here
        }
        assertJustOneVariantHasBeenStored();
    }

    @Test
    public void saveDifferentVariants() {
        dbsnpClusteredVariantWriter.write(Chunk.of(variantEntity1, variantEntity2, variantEntity3));
        assertAllUniqueVariantsHaveBeenStored();
    }

    private void assertAllUniqueVariantsHaveBeenStored() {
        List<DbsnpClusteredVariantEntity> storedVariants = mongoTemplate.find(new Query(),
                DbsnpClusteredVariantEntity.class);
        assertEquals(3, storedVariants.size());
        assertFalse(storedVariants.contains(duplicateVariantEntity1));
        assertEquals(ACCESSION_1, storedVariants.get(0).getAccession());
        assertEquals(ACCESSION_2, storedVariants.get(1).getAccession());
        assertEquals(ACCESSION_3, storedVariants.get(2).getAccession());
        assertEquals(clusteredVariant1, storedVariants.get(0).getModel());
        assertEquals(clusteredVariant2, storedVariants.get(1).getModel());
        assertEquals(clusteredVariant3, storedVariants.get(2).getModel());
        assertEquals(3, importCounts.getClusteredVariantsWritten());
    }

    @Test
    public void allNonDuplicatedRecordsWillBeWritten() {
        List<DbsnpClusteredVariantEntity> batch = Arrays.asList(variantEntity1, variantEntity2, duplicateVariantEntity1,
                variantEntity3);

        try {
            dbsnpClusteredVariantWriter.write(new Chunk<>(batch));
            fail();
        } catch (Exception e) {
            // it's correct and expected that an exception is thrown here
        }
        assertAllUniqueVariantsHaveBeenStored();
    }
}
