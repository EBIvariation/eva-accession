/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.deprecate.batch.io;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.MongoConfiguration;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.deprecate.test.configuration.MongoTestConfiguration;

import java.util.ArrayList;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup.ASSEMBLY;
import static uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup.STUDY1;
import static uk.ac.ebi.eva.accession.deprecate.MongoTestDatabaseSetup.populateTestDB;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:study-submitted-variants-test.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class StudySubmittedVariantsReaderTest extends MongoTestContainerHelper {
    private static final String ID_1 = "hash5";

    private static final String ID_2 = "hash6";

    private static final String ID_3 = "hash7";

    private static final int CHUNK_SIZE = 5;

    private ExecutionContext executionContext;

    private StudySubmittedVariantsReader reader;

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        populateTestDB(this.mongoTemplate);
        executionContext = new ExecutionContext();
        reader = new StudySubmittedVariantsReader(ASSEMBLY, STUDY1, mongoTemplate, CHUNK_SIZE);
        reader.open(executionContext);
    }

    @AfterEach
    public void tearDown() {
        mongoTemplate.getDb().drop();
        reader.close();
    }

    @Test
    public void readStudySubmittedVariants() {
        List<SubmittedVariantEntity> variants = readIntoList();
        assertEquals(3, variants.size());
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_1)));
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_2)));
        assertTrue(variants.stream().anyMatch(x -> x.getId().equals(ID_3)));
    }

    private List<SubmittedVariantEntity> readIntoList() {
        SubmittedVariantEntity variant;
        List<SubmittedVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }
}
