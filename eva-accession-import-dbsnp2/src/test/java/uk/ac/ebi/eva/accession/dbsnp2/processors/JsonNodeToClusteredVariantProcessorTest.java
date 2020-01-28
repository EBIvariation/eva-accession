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
package uk.ac.ebi.eva.accession.dbsnp2.processors;

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
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp2.io.BzipLazyResource;
import uk.ac.ebi.eva.accession.dbsnp2.io.JsonNodeLineMapper;
import uk.ac.ebi.eva.accession.dbsnp2.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp2.test.BatchTestConfiguration;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static uk.ac.ebi.eva.accession.dbsnp2.configuration.BeanNames.DBSNP_JSON_VARIANT_READER;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = {BatchTestConfiguration.class})
@TestExecutionListeners({DependencyInjectionTestExecutionListener.class,
        StepScopeTestExecutionListener.class})
@TestPropertySource({"classpath:application.properties"})
public class JsonNodeToClusteredVariantProcessorTest {

    @Autowired
    @Qualifier(DBSNP_JSON_VARIANT_READER)
    private FlatFileItemReader<JsonNode> reader;
    @Autowired
    private InputParameters inputParameters;
    private JsonNodeToClusteredVariantProcessor processor;
    private List<JsonNode> variants = new ArrayList<>();
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        reader.setResource(new BzipLazyResource(
            new File("src/test/resources/input-files/test-dbsnp.json.bz2")));
        reader.open(new ExecutionContext());
        JsonNode variant;
        while ((variant = reader.read()) != null) {
            variants.add(variant);
        }
        processor = new JsonNodeToClusteredVariantProcessor(inputParameters.getRefseqAssembly(),
                                                            inputParameters.getGenbankAssembly());
    }

    @Test
    public void variantTypeSNV() {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
            getFilteredDbsnpClusteredVariantEntities(VariantType.SNV);
        assertEquals(6, filteredClusteredVariants.size());
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

    @Test
    public void variantAttributesDerivationCheck() {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
            getFilteredDbsnpClusteredVariantEntities(VariantType.INDEL);
        List<DbsnpClusteredVariantEntity> variant3906List = filteredClusteredVariants.stream()
            .filter(dbsnpClusteredVariantEntity -> dbsnpClusteredVariantEntity.getAccession() == 3906L)
            .collect(Collectors.toList());
        assertEquals(1, variant3906List.size());
        DbsnpClusteredVariantEntity variant3906 = variant3906List.get(0);
        assertEquals("GCA_000001405.27", variant3906.getAssemblyAccession());
        assertEquals(9606L, variant3906.getTaxonomyAccession());
        assertEquals("NC_000024.10", variant3906.getContig());
        assertEquals(19562813L, variant3906.getStart());
        assertEquals("2000-09-19T17:02", String.valueOf(variant3906.getCreatedDate()));
    }

    @Test
    public void nullForNoPTLPCaseCheck() throws Exception {
        reader = new FlatFileItemReader<JsonNode>();
        reader.setLineMapper(new JsonNodeLineMapper());
        reader.setResource(new BzipLazyResource(
            new File("src/test/resources/input-files/test-dbsnp-no-ptlp.json.bz2")));
        reader.open(new ExecutionContext());
        JsonNode variant = reader.read();
        assertNull(processor.process(variant));
    }


    /**
     * Variants in the dbSNP JSON where there are multiple assemblies in the array in the path:
     * primary_snapshot_data -> placements_with_allele -> placement_annot -> seq_id_traits_by_assembly
     * @see <a href="https://api.ncbi.nlm.nih.gov/variation/v0/refsnp/9743">RS ID example</a>
     * @see <a href="https://ncbijira.ncbi.nlm.nih.gov/browse/VR-199">NCBI JIRA issue</a>
     **/
    @Test
    public void variantsWithMultipleTopLevelAssembliesProcessed() throws Exception {
        List<DbsnpClusteredVariantEntity> filteredClusteredVariants =
                getFilteredDbsnpClusteredVariantEntities(VariantType.SNV);
        assertEquals(1,
                     filteredClusteredVariants.stream()
                                              .filter(dbsnpClusteredVariantEntity ->
                                                              dbsnpClusteredVariantEntity.getAccession().equals(9743L))
                                              .count());
    }

    public List<DbsnpClusteredVariantEntity> getFilteredDbsnpClusteredVariantEntities(VariantType type) {
        return variants.stream()
            .map(variant -> processor.process(variant))
            .filter(Objects::nonNull)
            .filter(dbsnpClusteredVariantEntity -> dbsnpClusteredVariantEntity.getType().equals(type))
            .collect(Collectors.toList());
    }
}
