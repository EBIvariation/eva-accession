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
package uk.ac.ebi.eva.accession.dbsnp2.io;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.test.StepScopeTestExecutionListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp2.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_READER;

@TestPropertySource({"classpath:application.properties"})
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestExecutionListeners({DependencyInjectionTestExecutionListener.class,
    StepScopeTestExecutionListener.class})
@RunWith(SpringJUnit4ClassRunner.class)
public class JsonNodeToClusteredVariantProcessorTest {

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_READER)
    private FlatFileItemReader<JsonNode> reader;

    private JsonNodeToClusteredVariantProcessor processor;

    private List<JsonNode> variants = new ArrayList<>();
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        reader.setResource(new BzipLazyResource(new File("src/test/resources/input-files/test-dbsnp.json.bz2")));
        reader.open(new ExecutionContext());
        JsonNode variant;
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        processor = new JsonNodeToClusteredVariantProcessor();
    }

    @Test
    public void variantTypeSNV() {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
            getFilteredDbsnpClusteredVariantEntities(VariantType.SNV);
        assertEquals(5, filteredClusteredVariants.size());
    }

    @Test
    public void variantTypeMNV() {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
            getFilteredDbsnpClusteredVariantEntities(VariantType.MNV);
        assertEquals(5, filteredClusteredVariants.size());
    }

    @Test
    public void variantTypeINS() {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
            getFilteredDbsnpClusteredVariantEntities(VariantType.INS);
        assertEquals(5, filteredClusteredVariants.size());
    }

    @Test
    public void variantTypeDEL() {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
            getFilteredDbsnpClusteredVariantEntities(VariantType.DEL);
        assertEquals(5, filteredClusteredVariants.size());
    }

    @Test
    public void variantTypeINDEL() {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
            getFilteredDbsnpClusteredVariantEntities(VariantType.INDEL);
        assertEquals(5, filteredClusteredVariants.size());
    }



    public List<DbsnpClusteredVariantEntity> getFilteredDbsnpClusteredVariantEntities(VariantType type) {
        return variants.stream()
            .map(variant -> processor.process(variant))
            .filter(dbsnpClusteredVariantEntity -> dbsnpClusteredVariantEntity.getType().equals(type))
            .collect(Collectors.toList());
    }
}
