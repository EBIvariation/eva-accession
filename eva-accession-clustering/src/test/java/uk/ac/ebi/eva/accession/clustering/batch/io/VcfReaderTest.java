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
package uk.ac.ebi.eva.accession.clustering.batch.io;

import org.junit.Test;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.batch.item.ItemStreamReader;

import uk.ac.ebi.eva.accession.clustering.configuration.batch.io.VcfReaderConfiguration;
import uk.ac.ebi.eva.accession.clustering.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.AbstractVariantSourceEntry;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.anything;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.remapping.batch.io.VariantContextWriter.PROJECT_KEY;
import static uk.ac.ebi.eva.accession.remapping.batch.io.VariantContextWriter.RS_KEY;

public class VcfReaderTest {

    public static final String PROJECT = "PRJ_1";

    @Test
    public void simpleReading() throws Exception {
        ItemStreamReader<Variant> variantStreamReader = createReader();
        List<Variant> variants = consumeReader(variantStreamReader);
        assertEquals(5, variants.size());
    }

    private ItemStreamReader<Variant> createReader() throws IOException {
        InputParameters parameters = new InputParameters();
        parameters.setVcf("src/test/resources/input-files/vcf/aggregated_accessioned.vcf.gz");
        VcfReaderConfiguration vcfReaderConfiguration = new VcfReaderConfiguration();
        VcfReader vcfReader = vcfReaderConfiguration.vcfReader(parameters);
        return vcfReaderConfiguration.unwindingReader(vcfReader);
    }

    private List<Variant> consumeReader(ItemStreamReader<Variant> variantStreamReader) throws Exception {
        Variant variant;
        List<Variant> variants = new ArrayList<>();
        variantStreamReader.open(new ExecutionContext());
        while((variant = variantStreamReader.read()) != null) {
            variants.add(variant);
        }
        return variants;
    }

    @Test
    public void readingProjectAttribute() throws Exception {
        ItemStreamReader<Variant> variantStreamReader = createReader();
        List<Variant> variants = consumeReader(variantStreamReader);
        assertEquals(5, variants.size());
        List<Map<? extends String, ? extends String>> attributes = getSingleSourceAttributes(variants);
        assertThat(attributes, not(empty()));
        assertThat(attributes, everyItem(hasEntry(PROJECT_KEY, PROJECT)));
    }

    @Test
    public void readingRsAttribute() throws Exception {
        ItemStreamReader<Variant> variantStreamReader = createReader();
        List<Variant> variants = consumeReader(variantStreamReader);
        assertEquals(5, variants.size());
        List<Map<? extends String, ? extends String>> attributes = getSingleSourceAttributes(variants);
        assertThat(attributes, not(empty()));
        assertThat(attributes, everyItem(hasEntry(is(RS_KEY), anything())));
    }

    private List<Map<? extends String, ? extends String>> getSingleSourceAttributes(List<Variant> variants) {
        assertThat(variants.stream().map(v -> v.getSourceEntries().size()).collect(toList()), everyItem(is(1)));
        return variants.stream()
                       .map(Variant::getSourceEntries)
                       .flatMap(Collection::stream)
                       .map(AbstractVariantSourceEntry::getAttributes)
                       .collect(toList());
    }
}
