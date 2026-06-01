/*
 * Copyright 2020 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.core.model.eva;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Collections;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
@TestPropertySource("classpath:test-model.properties")
public class ClusteredVariantEntityIntegrationTest extends MongoTestContainerHelper {

    private static final String TEST_DB = "test-db";

    private Function<IClusteredVariant, String> clusteredHashingFunction;

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        clusteredHashingFunction = new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void variantWithMapWeightStoredInMongo() {
        assertMapWeight(2, 2);
    }

    @Test
    public void variantNoMapWeightStoredInMongo() {
        assertMapWeight(null, null);
    }

    /**
     * Mapping weight of 1 should be stored as null in mongo
     */
    @Test
    public void mapWeightOneStoredAsNullInMongo() {
        assertMapWeight(null, 1);
    }

    private void assertMapWeight(Integer expectedMapWeightFromMongo, Integer mapWeight) {
        ClusteredVariantEntity rs = createClusteredVariantEntity(mapWeight);
        mongoTemplate.insert(Collections.singletonList(rs), ClusteredVariantEntity.class);

        ClusteredVariantEntity variantEntity = mongoTemplate.find(new Query(), ClusteredVariantEntity.class).get(0);
        assertEquals(expectedMapWeightFromMongo, variantEntity.getMapWeight());
    }

    private ClusteredVariantEntity createClusteredVariantEntity(Integer mapWeight) {
        ClusteredVariant variant = createClusteredVariant();
        String hash = clusteredHashingFunction.apply(variant);
        ClusteredVariantEntity variantEntity = new ClusteredVariantEntity(3000000000L, hash, "asm", 1000, "contig", 100,
                VariantType.SNV, false, null, 1, mapWeight);
        return variantEntity;
    }

    private ClusteredVariant createClusteredVariant() {
        return new ClusteredVariant("asm", 1000, "contig", 100, VariantType.SNV, false, null);
    }
}
