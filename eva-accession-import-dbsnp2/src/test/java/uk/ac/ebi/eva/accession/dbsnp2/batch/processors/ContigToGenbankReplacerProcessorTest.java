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
package uk.ac.ebi.eva.accession.dbsnp2.batch.processors;

import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.ExpectedException;
import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.time.LocalDateTime;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class ContigToGenbankReplacerProcessorTest {

    private ContigToGenbankReplacerProcessor processor;

    private Function<IClusteredVariant, String> hashingFunction =
            new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    
    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        String fileString = ContigToGenbankReplacerProcessorTest.class.getResource(
                "/input-files/GCF_000001405.38_GRCh38.p12_assembly_report.txt").toString();
        ContigMapping contigMapping = new ContigMapping(fileString);

        processor = new ContigToGenbankReplacerProcessor(contigMapping);
    }

    @Test
    public void contigGenbank() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("CM000686.2");
        assertEquals("CM000686.2", processor.process(variant).getContig());
    }

    @Test
    public void contigChrToGenbank() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("chrY");
        assertEquals("CM000686.2", processor.process(variant).getContig());
    }

    @Test
    public void contigRefseqToGenbank() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("NC_000024.10");
        assertEquals("CM000686.2", processor.process(variant).getContig());
    }

    @Test
    public void genbankAndRefseqNotEquivalents() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("chr3");
        thrown.expect(IllegalStateException.class);
        assertEquals("chr3", processor.process(variant).getContig());
    }

    @Test
    public void genbankAndRefseqNotEquivalentsRefseqNotPresent() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("chrUn_KI270752v1");
        assertEquals("KI270752.1", processor.process(variant).getContig());
    }

    @Test
    public void genbankAndRefseqNotEquivalentsGenbankNotPresent() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("tstchr2");
        assertEquals("tstchr2", processor.process(variant).getContig());
    }

    @Test
    public void genbankAndRefseqNotEquivalentsNonePresent() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("tstchr3");
        assertEquals("tstchr3", processor.process(variant).getContig());
    }

    @Test
    public void contigNotFoundInAssemblyReport() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("chr");
        thrown.expect(IllegalStateException.class);
        assertEquals("chr", processor.process(variant).getContig());
    }

    @Test
    public void noGenbankDontConvert() throws Exception {
        DbsnpClusteredVariantEntity variant = buildMockVariant("tstchr4");
        assertEquals("tstchr4", processor.process(variant).getContig());
    }

    @Test
    public void updatedHashedMessageAfterContigReplacement() throws Exception {
        DbsnpClusteredVariantEntity processedVariant = processor.process(buildMockVariant("NC_000024.10"));
        assertEquals(new SHA1HashingFunction().apply("1_CM000686.2_1_SNV"),
                     processedVariant.getHashedMessage());

        processedVariant = processor.process(buildMockVariant("chrY"));
        assertEquals(new SHA1HashingFunction().apply("1_CM000686.2_1_SNV"),
                     processedVariant.getHashedMessage());

        processedVariant = processor.process(buildMockVariant("chrUn_KI270752v1"));
        assertEquals(new SHA1HashingFunction().apply("1_KI270752.1_1_SNV"),
                     processedVariant.getHashedMessage());

        processedVariant = processor.process(buildMockVariant("tstchr2"));
        assertEquals(new SHA1HashingFunction().apply("1_tstchr2_1_SNV"),
                     processedVariant.getHashedMessage());
    }

    private DbsnpClusteredVariantEntity buildMockVariant(String originalChromosome) {
        ClusteredVariant newVariant = new ClusteredVariant("1",
                                                           9606,
                                                           originalChromosome,
                                                           1,
                                                           VariantType.SNV,
                                                           Boolean.FALSE,
                                                           LocalDateTime.now());
        return new DbsnpClusteredVariantEntity(1L,
                                               hashingFunction.apply(newVariant),
                                               newVariant,
                                               1);
    }
}
