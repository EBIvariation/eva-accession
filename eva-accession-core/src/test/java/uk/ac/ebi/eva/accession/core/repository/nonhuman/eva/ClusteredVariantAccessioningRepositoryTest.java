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
package uk.ac.ebi.eva.accession.core.repository.nonhuman.eva;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.AccessionProjection;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(SpringExtension.class)
@DataJpaTest
@TestPropertySource("classpath:ss-accession-test.properties")
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
public class ClusteredVariantAccessioningRepositoryTest extends MongoTestContainerHelper {

    private ClusteredVariant clusteredVariant;

    private ClusteredVariant newClusteredVariant;

    @Autowired
    private ClusteredVariantAccessioningRepository repository;

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() {
        mongoTemplate.getDb().drop();
        clusteredVariant = new ClusteredVariant("GCA_000003055.3", 9913, "21", 20800319, VariantType.SNV, false,
                LocalDateTime.of(2000, Month.SEPTEMBER, 19, 18, 2, 0));

        newClusteredVariant = new ClusteredVariant("assembly", 1111, "contig_2", 100, VariantType.SNV, false,
                LocalDateTime.of(2000, Month.SEPTEMBER, 19, 18, 2, 0));

    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }


    @Test
    public void queryAccessionRange() {
        long firstAccession = 1000L;
        long secondAccession = 1002L;
        List<ClusteredVariantEntity> variants = Arrays.asList(
                new ClusteredVariantEntity(firstAccession, "hash-1", clusteredVariant, 1),
                new ClusteredVariantEntity(secondAccession, "hash-2", newClusteredVariant, 1));

        repository.saveAll(variants);

        assertAccessionsEquals(Arrays.asList(firstAccession),
                repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1000L, 1000L));

        assertAccessionsEquals(Arrays.asList(),
                repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1001L, 1001L));

        assertAccessionsEquals(Arrays.asList(firstAccession, secondAccession),
                repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1000L, 1005L));

        assertAccessionsEquals(Arrays.asList(secondAccession),
                repository.findByAccessionGreaterThanEqualAndAccessionLessThanEqual(1001L, 1005L));
    }

    private void assertAccessionsEquals(List<Long> expectedAccessions,
                                        List<AccessionProjection<Long>> accessionsProjection) {
        assertEquals(new TreeSet<>(expectedAccessions),
                accessionsProjection.stream().map(AccessionProjection::getAccession).collect(Collectors.toSet()));
    }

    @Test
    public void testQueryAssemblyAndAccessionFilter() {
        long firstAccession = 1000L;
        long secondAccession = 1002L;
        List<ClusteredVariantEntity> variants = Arrays.asList(
                new ClusteredVariantEntity(firstAccession, "hash-1", clusteredVariant, 1),
                new ClusteredVariantEntity(secondAccession, "hash-2", newClusteredVariant, 1));

        repository.saveAll(variants);

        List<Long> accessions = repository.findByAssemblyAccessionAndAccessionIn(
                "assembly", Arrays.asList(firstAccession, secondAccession)
        ).stream().map(AccessionedDocument::getAccession).collect(Collectors.toList());
        assertEquals(1, accessions.size());

        accessions = repository.findByAssemblyAccessionAndAccessionIn(
                "assembly", Collections.singletonList(firstAccession)
        ).stream().map(AccessionedDocument::getAccession).collect(Collectors.toList());
        assertEquals(0, accessions.size());
    }
}
