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
package uk.ac.ebi.eva.accession.clustering.batch.io;

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
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.accession.core.utils.MongoTestDataLoader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@TestPropertySource("classpath:clustering-pipeline-test.properties")
@ContextConfiguration(classes = {MongoConfiguration.class, MongoTestConfiguration.class})
public class StudyClusteringMongoReaderTest extends MongoTestContainerHelper {
    private static final String ASSEMBLY = "GCA_000000001.1";

    private static final List<String> PROJECTS = Arrays.asList("projectId_2", "projectId_3");

    private static final int CHUNK_SIZE = 5;

    private static final String SUBMITTED_VARIANT_ENTITY = "submittedVariantEntity";

    private static final String PROJECT_2_HASH = "9F05088C2058BC2AECFF8B904E439E2FD4C67F20";

    private static final String PROJECT_3_HASH = "9B9240E0488CA3960B2FFC161878DA7F21FC1756";

    private StudyClusteringMongoReader studyClusteringMongoReader;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private ResourceLoader resourceLoader;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();

        MongoTestDataLoader mongoTestDataLoader = new MongoTestDataLoader(mongoTemplate, resourceLoader);
        mongoTestDataLoader.load("/test-data/submittedVariantEntityStudyReader.json");
        mongoTestDataLoader.load("/test-data/dbsnpSubmittedVariantEntityMongoReader.json");

        ExecutionContext executionContext = new ExecutionContext();
        studyClusteringMongoReader = new StudyClusteringMongoReader(mongoTemplate, ASSEMBLY, PROJECTS, CHUNK_SIZE);
        studyClusteringMongoReader.open(executionContext);
    }

    @AfterEach
    public void tearDown() {
        studyClusteringMongoReader.close();
        mongoTemplate.getDb().drop();
    }

    @Test
    public void readSubmittedVariantsPerStudy() {
        assertEquals(5, mongoTemplate.getCollection(SUBMITTED_VARIANT_ENTITY).countDocuments());
        List<SubmittedVariantEntity> variants = readIntoList(studyClusteringMongoReader);
        assertEquals(2, variants.size());

        Set<String> actualHashes = variants.stream().map(SubmittedVariantEntity::getId).collect(Collectors.toSet());
        Set<String> expectedHashes = Stream.of(PROJECT_2_HASH, PROJECT_3_HASH).collect(Collectors.toSet());
        assertEquals(expectedHashes, actualHashes);
    }

    private List<SubmittedVariantEntity> readIntoList(StudyClusteringMongoReader reader) {
        SubmittedVariantEntity variant;
        List<SubmittedVariantEntity> variants = new ArrayList<>();
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }
}