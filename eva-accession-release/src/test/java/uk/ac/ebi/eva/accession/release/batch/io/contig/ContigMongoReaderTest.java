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

package uk.ac.ebi.eva.accession.release.batch.io.contig;

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
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;
import uk.ac.ebi.eva.accession.release.collectionNames.DbsnpCollectionNames;
import uk.ac.ebi.eva.accession.release.collectionNames.EvaCollectionNames;
import uk.ac.ebi.eva.accession.release.test.configuration.MongoTestConfiguration;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:application.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class ContigMongoReaderTest extends MongoTestContainerHelper {

    private static final String TEST_DB = "test-db";

    private static final String ASSEMBLY_ACCESSION = "GCA_000409795.2";

    private static final int TAXONOMY_ACCESSION = 60711;

    @Autowired
    private MongoClient mongoClient;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load("/test-data/dbsnpClusteredVariantEntity.json");
        mongoTestDataLoader.load("/test-data/dbsnpClusteredVariantOperationEntity.json");
        mongoTestDataLoader.load("/test-data/submittedVariantEntity.json");
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
    }

    @Test
    public void basicActiveContigsRead() {
        ContigMongoReader reader = ContigMongoReader.activeContigReader(ASSEMBLY_ACCESSION, TAXONOMY_ACCESSION,
                mongoClient, TEST_DB, new EvaCollectionNames());
        reader.open(new ExecutionContext());
        String contig;
        List<String> contigs = new ArrayList<>();
        while ((contig = reader.read()) != null) {
            contigs.add(contig);
        }
        assertEquals(new HashSet<>(Arrays.asList("CM001954.1", "CM001941.2")), new HashSet<>(contigs));
    }

    @Test
    public void basicMergedContigsRead() {
        ContigMongoReader reader = ContigMongoReader.mergedContigReader(ASSEMBLY_ACCESSION, mongoClient,
                TEST_DB, new DbsnpCollectionNames());
        reader.open(new ExecutionContext());
        String contig;
        List<String> contigs = new ArrayList<>();
        while ((contig = reader.read()) != null) {
            contigs.add(contig);
        }
        assertEquals(new HashSet<>(Arrays.asList("CM001954.1", "CM001941.2")), new HashSet<>(contigs));
    }

    @Test
    public void basicMultimapContigsRead() {
        ContigMongoReader reader = ContigMongoReader.multimapContigReader(ASSEMBLY_ACCESSION, mongoClient, TEST_DB,
                new DbsnpCollectionNames());
        reader.open(new ExecutionContext());
        String contig;
        List<String> contigs = new ArrayList<>();
        while ((contig = reader.read()) != null) {
            contigs.add(contig);
        }
        assertEquals(new HashSet<>(Arrays.asList("CM001954.1")), new HashSet<>(contigs));
    }
}
