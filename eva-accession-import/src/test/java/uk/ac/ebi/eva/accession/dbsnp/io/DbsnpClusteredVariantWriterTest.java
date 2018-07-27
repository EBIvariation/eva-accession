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
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.test.MongoTestConfiguration;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class DbsnpClusteredVariantWriterTest {
    private static final int TAXONOMY_1 = 3880;

    private static final int TAXONOMY_2 = 3882;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final long EXPECTED_ACCESSION_2 = 10000000001L;

    private static final int START_1 = 100;

    private static final VariantType VARIANT_TYPE = VariantType.SNV;

    private static final Boolean VALIDATED = false;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private DbsnpClusteredVariantWriter dbsnpClusteredVariantWriter;

    @Autowired
    private MongoTemplate mongoTemplate;

    private Function<IClusteredVariant, String> hashingFunction;

    @Before
    public void setUp() throws Exception {
        dbsnpClusteredVariantWriter = new DbsnpClusteredVariantWriter(mongoTemplate);
        hashingFunction = new DbsnpClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DbsnpClusteredVariantEntity.class);
    }

    @Test
    public void saveSingleAccession() throws Exception {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity variant = new DbsnpClusteredVariantEntity(EXPECTED_ACCESSION,
                                                                              hashingFunction.apply(clusteredVariant),
                                                                              clusteredVariant);

        dbsnpClusteredVariantWriter.write(Collections.singletonList(variant));

        List<DbsnpClusteredVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.get(0).getAccession());

        assertEquals(clusteredVariant, accessions.get(0).getModel());
    }

    @Test
    public void saveDifferentTaxonomies() throws Exception {
        ClusteredVariant firstClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                      VARIANT_TYPE, VALIDATED);
        ClusteredVariant secondClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_2, "contig", START_1,
                                                                       VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity firstVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(firstClusteredVariant), firstClusteredVariant);
        DbsnpClusteredVariantEntity secondVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION_2, hashingFunction.apply(secondClusteredVariant), secondClusteredVariant);

        dbsnpClusteredVariantWriter.write(Arrays.asList(firstVariant, secondVariant));

        List<DbsnpClusteredVariantEntity> accessions = mongoTemplate.find(new Query(),
                                                                          DbsnpClusteredVariantEntity.class);
        assertEquals(2, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.get(0).getAccession());
        assertEquals(EXPECTED_ACCESSION_2, (long) accessions.get(1).getAccession());

        assertEquals(firstClusteredVariant, accessions.get(0).getModel());
        assertEquals(secondClusteredVariant, accessions.get(1).getModel());
    }

    @Test
    public void failsOnDuplicateVariant() throws Exception {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, VALIDATED);
        DbsnpClusteredVariantEntity variant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(clusteredVariant), clusteredVariant);

        thrown.expect(RuntimeException.class);
        dbsnpClusteredVariantWriter.write(Arrays.asList(variant, variant));
    }

    @Test
    public void failsOnDuplicateNonIdenticalVariant() throws Exception {
        ClusteredVariant clusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, false);
        ClusteredVariant duplicateClusteredVariant = new ClusteredVariant("assembly", TAXONOMY_1, "contig", START_1,
                                                                 VARIANT_TYPE, true);
        DbsnpClusteredVariantEntity variant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(clusteredVariant), clusteredVariant);
        DbsnpClusteredVariantEntity duplicateVariant = new DbsnpClusteredVariantEntity(
                EXPECTED_ACCESSION, hashingFunction.apply(duplicateClusteredVariant), duplicateClusteredVariant);

        thrown.expect(RuntimeException.class);
        dbsnpClusteredVariantWriter.write(Arrays.asList(variant, duplicateVariant));
    }
}
