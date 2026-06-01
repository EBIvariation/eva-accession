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

package uk.ac.ebi.eva.accession.release.batch.io.merged_deprecated;

import com.mongodb.client.MongoClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.accession.release.collectionNames.DbsnpCollectionNames;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:application.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class MergedDeprecatedVariantMongoReaderTest extends MongoTestContainerHelper {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000409795.2";

    private static final int TAXONOMY = 60711;

    private static final int CHUNK_SIZE = 5;

    @Autowired
    private MongoTemplate mongoTemplate;


    @Autowired
    private ResourceLoader resourceLoader;

    @Autowired
    private MongoClient mongoClient;

    private MergedDeprecatedVariantMongoReader<DbsnpClusteredVariantOperationEntity> reader;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();

        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load("/test-data/dbsnpClusteredVariantOperationEntity.json");
        mongoTestDataLoader.load("/test-data/dbsnpSubmittedVariantOperationEntity.json");
        mongoTestDataLoader.load("/test-data/dbsnpClusteredVariantEntity.json");
        mongoTestDataLoader.load("/test-data/dbsnpSubmittedVariantEntity.json");

        ExecutionContext executionContext = new ExecutionContext();
        reader = new DbsnpMergedDeprecatedVariantMongoReader(ASSEMBLY, TAXONOMY, mongoClient, TEST_DB,
                mongoTemplate.getConverter(), CHUNK_SIZE,
                new DbsnpCollectionNames());
        reader.open(executionContext);
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
        reader.close();
    }

    @Test
    public void basicRead() throws Exception {
        List<DbsnpClusteredVariantOperationEntity> variants = readIntoList();
        assertEquals(2, variants.size());

        for (DbsnpClusteredVariantOperationEntity variant : variants) {
            assertEquals(EventType.MERGED, variant.getEventType());
            variant.getInactiveObjects().stream().forEach(o -> assertEquals(ASSEMBLY, o.getAssemblyAccession()));
        }
    }

    private List<DbsnpClusteredVariantOperationEntity> readIntoList() throws Exception {
        List<DbsnpClusteredVariantOperationEntity> variants = new ArrayList<>();
        DbsnpClusteredVariantOperationEntity variant;

        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }

        return variants;
    }

}
