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
package uk.ac.ebi.eva.accession.pipeline.steps.processors;

import org.junit.Before;
import org.junit.Test;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.IVariantSourceEntry;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static uk.ac.ebi.eva.accession.pipeline.steps.processors.ContigToGenbankReplacerProcessor.ORIGINAL_CHROMOSOME;

public class ContigToGenbankReplacerProcessorTest {

    private ContigToGenbankReplacerProcessor processor;

    @Before
    public void setUp() throws Exception {
        String fileString = ContigToGenbankReplacerProcessorTest.class.getResource(
                "/input-files/assembly-report/assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        processor = new ContigToGenbankReplacerProcessor(contigMapping);
    }

    @Test
    public void ContigGenbank() throws Exception {
        IVariant variant = buildMockVariant("CM000093.4");
        assertEquals("CM000093.4", processor.process(variant).getChromosome());
    }

    @Test
    public void ContigChrToGenbank() throws Exception {
        IVariant variant = buildMockVariant("chr2");
        assertEquals("CM000094.4", processor.process(variant).getChromosome());
    }

    @Test
    public void ContigRefseqToGenbank() throws Exception {
        IVariant variant = buildMockVariant("NW_003763476.1");
        assertEquals("CM000093.4", processor.process(variant).getChromosome());
    }

    @Test
    public void GenbankAndRefseqNotEquivalents() throws Exception {
        IVariant variant = buildMockVariant("chr3");
        assertEquals("chr3", processor.process(variant).getChromosome());
    }

    @Test
    public void GenbankAndRefseqNotEquivalentsRefseqNotPresent() throws Exception {
        IVariant variant = buildMockVariant("chr6");
        assertEquals("CM000096.4", processor.process(variant).getChromosome());
    }

    @Test
    public void GenbankAndRefseqNotEquivalentsGenbankNotPresent() throws Exception {
        IVariant variant = buildMockVariant("chr5");
        assertEquals("chr5", processor.process(variant).getChromosome());
    }

    @Test
    public void GenbankAndRefseqNotEquivalentsNonePresent() throws Exception {
        IVariant variant = buildMockVariant("chr7");
        assertEquals("chr7", processor.process(variant).getChromosome());
    }

    @Test
    public void ContigNotFoundInAssemblyReport() throws Exception {
        IVariant variant = buildMockVariant("chr");
        assertEquals("chr", processor.process(variant).getChromosome());
    }

    @Test
    public void NoGenbankDontConvert() throws Exception {
        IVariant variant = buildMockVariant("chr4");
        assertEquals("chr4", processor.process(variant).getChromosome());
    }

    @Test
    public void keepOriginalChromosomeInInfo() throws Exception {
        String originalChromosome = "chr1";
        Variant variant = buildMockVariant(originalChromosome);

        Set<String> originalChromosomes = processor.process(variant)
                                                   .getSourceEntries()
                                                   .stream()
                                                   .map(e -> e.getAttributes().get(ORIGINAL_CHROMOSOME))
                                                   .collect(Collectors.toSet());

        assertEquals(Collections.singleton(originalChromosome), originalChromosomes);
    }

    private Variant buildMockVariant(String originalChromosome) {
        Variant variant = new Variant(originalChromosome, 1, 1, "A", "T");
        variant.addSourceEntry(new VariantSourceEntry("fileId", "studyId"));
        return variant;
    }
}