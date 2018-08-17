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
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.configuration.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp.io.DbsnpClusteredVariantDeclusteredWriter.DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:test-variants-writer.properties")
@ContextConfiguration(classes = {MongoConfiguration.class})
public class DbsnpClusteredVariantDeclusteredWriterTest {

    private static final int TAXONOMY = 3880;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final int START = 100;

    private static final VariantType VARIANT_TYPE = VariantType.SNV;

    private static final Boolean VALIDATED = false;

    private DbsnpClusteredVariantDeclusteredWriter writer;

    private Function<IClusteredVariant, String> hashingFunction;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Before
    public void setUp() throws Exception {
        writer = new DbsnpClusteredVariantDeclusteredWriter(mongoTemplate);
        hashingFunction = new DbsnpClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
        mongoTemplate.dropCollection(DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);
    }

    @Test
    public void writeDeclusteredRs() {
        DbsnpClusteredVariantEntity variant = newDbsnpClusteredVariantEntity(START, EXPECTED_ACCESSION);
        writer.write(Collections.singletonList(variant));

        List<DbsnpClusteredVariantEntity> rsEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);

        assertEquals(1, rsEntities.size());
        assertEquals(variant, rsEntities.get(0));
    }

    private DbsnpClusteredVariantEntity newDbsnpClusteredVariantEntity(int start, Long accession) {
        ClusteredVariant clusteredVariantDuplicated = new ClusteredVariant("assembly", TAXONOMY, "contig", start,
                VARIANT_TYPE, VALIDATED);
        return new DbsnpClusteredVariantEntity(accession,
                hashingFunction.apply(clusteredVariantDuplicated), clusteredVariantDuplicated);
    }

    @Test
    public void writeMultipleDistinctDeclusteredRss() {
        DbsnpClusteredVariantEntity variant1 = newDbsnpClusteredVariantEntity(START, EXPECTED_ACCESSION);
        DbsnpClusteredVariantEntity variant2 = newDbsnpClusteredVariantEntity(START+1, EXPECTED_ACCESSION+1);
        writer.write(Arrays.asList(variant1, variant2));

        List<DbsnpClusteredVariantEntity> rsEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);

        assertEquals(2, rsEntities.size());
        assertEquals(variant1, rsEntities.get(0));
    }

    @Test
    public void writeMultipleDuplicateDeclusteredRss() {
        DbsnpClusteredVariantEntity variant = newDbsnpClusteredVariantEntity(START, EXPECTED_ACCESSION);
        DbsnpClusteredVariantEntity variantDuplicated = newDbsnpClusteredVariantEntity(START, EXPECTED_ACCESSION);
        writer.write(Arrays.asList(variant, variantDuplicated));

        List<DbsnpClusteredVariantEntity> rsEntities = mongoTemplate.find
                (new Query(), DbsnpClusteredVariantEntity.class, DBSNP_CLUSTERED_VARIANT_DECLUSTERED_COLLECTION_NAME);

        assertEquals(1, rsEntities.size());
        assertEquals(variant, rsEntities.get(0));
    }

}
