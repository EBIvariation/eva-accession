/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core.service.nonhuman.eva;

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
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.ClusteredVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.test.configuration.nonhuman.MongoTestConfiguration;
import uk.ac.ebi.eva.accession.core.utils.MongoTestContainerHelper;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(SpringExtension.class)
@DataJpaTest
@TestPropertySource("classpath:rs-accession-test.properties")
@ContextConfiguration(classes = {ClusteredVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
public class ClusteredVariantMonotonicAccessioningServiceTest extends MongoTestContainerHelper {

    @Autowired
    private ClusteredVariantMonotonicAccessioningService clusteredVariantMonotonicAccessioningService;

    @Autowired
    private MongoTemplate mongoTemplate;

    @BeforeEach
    public void setUp() throws Exception {
        mongoTemplate.getDb().drop();
    }

    @AfterEach
    void cleanDb() {
        mongoTemplate.getDb().drop();
    }


    @Test
    public void service() throws AccessionCouldNotBeGeneratedException {
        ClusteredVariant clusteredVariant1 = new ClusteredVariant("asm1", 100, "chr1", 1, VariantType.SNV, false, null);
        ClusteredVariant clusteredVariant2 = new ClusteredVariant("asm1", 100, "chr1", 5, VariantType.SNV, false, null);
        List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> accessionedWrappers =
                clusteredVariantMonotonicAccessioningService.getOrCreate(Arrays.asList(clusteredVariant1,
                        clusteredVariant2), "test-application-instance-id");
        assertEquals(2, accessionedWrappers.size());
        assertTrue(isAccessionInResults(accessionedWrappers, 3000000000L));
        assertTrue(isAccessionInResults(accessionedWrappers, 3000000001L));
    }

    private boolean isAccessionInResults(List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> results,
                                         Long accession) {
        return results.stream().filter(a -> a.getAccession().equals(accession)).count() == 1;
    }
}