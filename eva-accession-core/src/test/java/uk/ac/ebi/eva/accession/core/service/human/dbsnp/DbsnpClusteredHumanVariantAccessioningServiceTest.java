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
package uk.ac.ebi.eva.accession.core.service.human.dbsnp;

import com.lordofthejars.nosqlunit.annotation.UsingDataSet;
import com.lordofthejars.nosqlunit.mongodb.MongoDbConfigurationBuilder;
import com.lordofthejars.nosqlunit.mongodb.MongoDbRule;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.test.configuration.human.MongoHumanTestConfiguration;
import uk.ac.ebi.eva.accession.core.test.rule.FixSpringMongoDbRule;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.time.Month;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@DataJpaTest
@TestPropertySource("classpath:ss-accession-test.properties")
@UsingDataSet(locations = {"/test-data/dbsnpClusteredVariantEntity.json", "/test-data/dbsnpClusteredVariantOperationEntity.json"})
@ContextConfiguration(classes = {MongoHumanTestConfiguration.class})
public class DbsnpClusteredHumanVariantAccessioningServiceTest {

    private static final Long HUMAN_ACTIVE_RS_ID_1 = 1118L;

    private static final Long HUMAN_ACTIVE_RS_ID_2 = 1314L;

    private static final Long HUMAN_ACTIVE_IN_OPERATIONS_RS_ID = 1475292486L;

    private static final ClusteredVariant CLUSTERED_VARIANT_EXPECTED_1 = new ClusteredVariant("GCA_000001405.27", 9606,
            "CM000684.2", 45565333L, VariantType.SNV, false, LocalDateTime.of(2000, Month.SEPTEMBER, 19, 18, 2, 0));

    private static final ClusteredVariant CLUSTERED_VARIANT_EXPECTED_2 = new ClusteredVariant("GCA_000001405.27", 9606,
            "CM000663.2", 72348112L, VariantType.INDEL, false, LocalDateTime.of(2017, Month.NOVEMBER, 11, 9, 55, 0));

    @Autowired
    @Qualifier("humanService")
    private DbsnpClusteredHumanVariantAccessioningService humanService;

    //Required by nosql-unit
    @Autowired
    private ApplicationContext applicationContext;

    @Rule
    public MongoDbRule mongoDbRule = new FixSpringMongoDbRule(
            MongoDbConfigurationBuilder.mongoDb().databaseName("human-variants-test").build());

    @Test
    public void getHumanActiveVariant() {
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                humanService.getAllByAccession(HUMAN_ACTIVE_RS_ID_1);
        assertEquals(1, clusteredVariants.size());
        ClusteredVariant clusteredVariant = clusteredVariants.get(0).getData();
        assertEquals(CLUSTERED_VARIANT_EXPECTED_1, clusteredVariant);
    }

    @Test
    @Ignore("humanService.getAllByAccession is not returning multiple variants yet")
    public void getHumanActiveMultipleVariants() {
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                humanService.getAllByAccession(HUMAN_ACTIVE_RS_ID_2);
        assertEquals(2, clusteredVariants.size());
    }

    @Test
    public void getHumanActiveVariantInOperations() {
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                humanService.getAllByAccession(HUMAN_ACTIVE_IN_OPERATIONS_RS_ID);
        assertEquals(1, clusteredVariants.size());
        ClusteredVariant clusteredVariant = clusteredVariants.get(0).getData();
        assertEquals(CLUSTERED_VARIANT_EXPECTED_2, clusteredVariant);
    }

    @Test
    public void nonExistingHumanVariant() {
        List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> clusteredVariants =
                humanService.getAllByAccession(1L);
        assertEquals(0, clusteredVariants.size());
        assertEquals(Collections.EMPTY_LIST, clusteredVariants);
    }

}