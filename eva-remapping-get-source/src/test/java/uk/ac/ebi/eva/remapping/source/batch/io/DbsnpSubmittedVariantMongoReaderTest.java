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

package uk.ac.ebi.eva.remapping.source.batch.io;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.ResourceLoader;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:application.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class DbsnpSubmittedVariantMongoReaderTest extends MongoTestContainerHelper {

    private static final String ASSEMBLY = "GCA_000409795.2";

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    private DbsnpSubmittedVariantMongoReader reader;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();

        new MongoTestDataLoader(mongoTemplate, resourceLoader).load("/test-data/dbsnpSubmittedVariantEntity.json");

        ExecutionContext executionContext = new ExecutionContext();
        reader = new DbsnpSubmittedVariantMongoReader(ASSEMBLY, mongoTemplate, null, 0);
        reader.open(executionContext);
    }

    @AfterEach
    public void tearDown() {
        reader.close();
        mongoTemplate.getDb().drop();
    }

    @Test
    public void basicRead() throws Exception {
        List<DbsnpSubmittedVariantEntity> variants = readIntoList(reader);
        assertEquals(10, variants.size());

        for (DbsnpSubmittedVariantEntity variant : variants) {
            assertEquals(ASSEMBLY, variant.getReferenceSequenceAccession());
        }
    }

    @Test
    public void queryByStudy() throws Exception {
        DbsnpSubmittedVariantMongoReader reader = new DbsnpSubmittedVariantMongoReader(
                ASSEMBLY, mongoTemplate, Arrays.asList("PRJEB7923", "PRJEB7929"), 0);
        reader.open(new ExecutionContext());
        List<DbsnpSubmittedVariantEntity> variants = readIntoList(reader);
        assertEquals(6, variants.size());
    }

    @Test
    public void queryByStudyAndTaxonomy() throws Exception {
        DbsnpSubmittedVariantMongoReader reader = new DbsnpSubmittedVariantMongoReader(
                ASSEMBLY, mongoTemplate, Arrays.asList("PRJEB7923", "PRJEB7929"), 10000);
        reader.open(new ExecutionContext());
        List<DbsnpSubmittedVariantEntity> variants = readIntoList(reader);
        assertEquals(1, variants.size());
    }

    @Test
    public void queryByTaxonomy() throws Exception {
        DbsnpSubmittedVariantMongoReader reader = new DbsnpSubmittedVariantMongoReader(
                ASSEMBLY, mongoTemplate, null, 10000);
        reader.open(new ExecutionContext());
        List<DbsnpSubmittedVariantEntity> variants = readIntoList(reader);
        assertEquals(1, variants.size());
    }

    @Test
    public void queryByMissingStudy() throws Exception {
        DbsnpSubmittedVariantMongoReader reader = new DbsnpSubmittedVariantMongoReader(
                ASSEMBLY, mongoTemplate, Collections.singletonList("inexistent_PRJEB"), 0);

        reader.open(new ExecutionContext());
        List<DbsnpSubmittedVariantEntity> variants = readIntoList(reader);
        assertEquals(0, variants.size());
    }

    private <T> List<T> readIntoList(ItemReader<T> reader) throws Exception {
        List<T> variants = new ArrayList<>();
        T variant;

        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }

        return variants;
    }

}
