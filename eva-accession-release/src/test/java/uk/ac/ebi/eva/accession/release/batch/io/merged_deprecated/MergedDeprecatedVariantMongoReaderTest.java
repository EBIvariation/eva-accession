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

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import com.mongodb.MongoClient;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.release.batch.io.merged_deprecated.MergedDeprecatedVariantMongoReader;
import uk.ac.ebi.eva.accession.release.collectionNames.DbsnpCollectionNames;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.release.test.rule.FixSpringMongoDbRule;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@TestPropertySource("classpath:application.properties")
@UsingDataSet(locations = {
        "/test-data/dbsnpClusteredVariantOperationEntity.json",
        "/test-data/dbsnpClusteredVariantEntity.json",
        "/test-data/dbsnpSubmittedVariantEntity.json"
})
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class MergedDeprecatedVariantMongoReaderTest {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY = "GCA_000409795.2";

    private static final int TAXONOMY = 60711;

    private static final int CHUNK_SIZE = 5;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MongoClient mongoClient;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName(TEST_DB).build());

    private MergedDeprecatedVariantMongoReader<DbsnpClusteredVariantOperationEntity> reader;

    @Before
    public void setUp() {
        ExecutionContext executionContext = new ExecutionContext();
        reader = new DbsnpMergedDeprecatedVariantMongoReader(ASSEMBLY, TAXONOMY, mongoClient, TEST_DB,
                                                             mongoTemplate.getConverter(), CHUNK_SIZE,
                                                             new DbsnpCollectionNames());
        reader.open(executionContext);
    }

    @After
    public void tearDown() {
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
