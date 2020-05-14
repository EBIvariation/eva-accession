/*
 * Copyright 2016 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.remapping.batch.processors;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.assertj.core.util.Sets;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class SubmittedVariantToVariantContextProcessorTest {

    private static final String FILE_ID = "fileId";

    private static final String SNP_SEQUENCE_ONTOLOGY = "SO:0001483";

    private static final String DELETION_SEQUENCE_ONTOLOGY = "SO:0000159";

    private static final String INSERTION_SEQUENCE_ONTOLOGY = "SO:0000667";

    private static final String SEQUENCE_NAME_1 = "Chr1";

    private static final String GENBANK_ACCESSION_1 = "CM0001.1";

    private static final String ID = "rs123";

    private static final String STUDY_1 = "study_1";

    private static final String STUDY_2 = "study_2";

    private static final String VARIANT_CLASS_KEY = "VC";

    private static final String STUDY_ID_KEY = "SID";

    private SubmittedVariantToVariantContextProcessor variantConverter;

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        ContigMapping contigMapping = new ContigMapping(Collections.singletonList(
                new ContigSynonyms(SEQUENCE_NAME_1, "A", "A", GENBANK_ACCESSION_1, "A", "A", true)));
        variantConverter = new SubmittedVariantToVariantContextProcessor(contigMapping);
    }

    @Test
    public void singleStudySNV() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1000, "C", "A", SNP_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1000, 1000, ID, "C", "A", SNP_SEQUENCE_ONTOLOGY, STUDY_1);
    }

    private Variant buildVariant(String chr1, int start, String reference, String alternate,
                                 String sequenceOntologyTerm, String... studies) {
        Variant variant = new Variant(chr1, start, start + alternate.length(), reference, alternate);
        variant.setMainId(ID);
        for (String study : studies) {
            VariantSourceEntry sourceEntry = new VariantSourceEntry(study, FILE_ID);
            sourceEntry.addAttribute(VARIANT_CLASS_KEY, sequenceOntologyTerm);
            sourceEntry.addAttribute(STUDY_ID_KEY, study);
            variant.addSourceEntry(sourceEntry);
        }
        return variant;
    }

    private void assertVariantContext(VariantContext variantContext, String expectedChromosome, int expectedStart,
                                      int expectedEnd, String expectedId, String expectedReference,
                                      String expectedAlternate, String expectedSequenceOntology,
                                      String... expectedStudies) {
        assertEquals(expectedChromosome, variantContext.getContig());
        assertEquals(expectedStart, variantContext.getStart());
        assertEquals(expectedEnd, variantContext.getEnd());
        assertEquals(Allele.create(expectedReference, true), variantContext.getReference());
        assertEquals(Collections.singletonList(Allele.create(expectedAlternate, false)),
                     variantContext.getAlternateAlleles());
        assertEquals(expectedId, variantContext.getID());
        assertTrue(variantContext.getFilters().isEmpty());
        assertEquals(2, variantContext.getCommonInfo().getAttributes().size());

        assertEquals(expectedSequenceOntology, variantContext.getCommonInfo().getAttribute(VARIANT_CLASS_KEY));

        assertTrue(variantContext.getCommonInfo().hasAttribute(STUDY_ID_KEY));
        String[] studies = ((String) variantContext.getCommonInfo().getAttribute(STUDY_ID_KEY)).split(",");
        assertEquals(Sets.newLinkedHashSet(expectedStudies), Sets.newLinkedHashSet(studies));

        assertEquals(0, variantContext.getSampleNames().size());
    }

    @Test
    public void throwsIfAllelesAreEmpty() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1100, "", "G", SNP_SEQUENCE_ONTOLOGY, STUDY_1);
        expectedException.expect(IllegalArgumentException.class);
        variantConverter.process(variant);
    }

    @Test
    public void singleStudySingleNucleotideInsertion() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1100, "T", "TG", SNP_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1100, 1100, ID, "T", "TG", SNP_SEQUENCE_ONTOLOGY,
                             STUDY_1);
    }

    @Test
    public void singleStudySeveralNucleotidesInsertion() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1100, "T", "TGA", INSERTION_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1100, 1100, ID, "T", "TGA", INSERTION_SEQUENCE_ONTOLOGY,
                             STUDY_1);
    }

    @Test
    public void singleStudySingleNucleotideDeletion() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1100, "TA", "T", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1100, 1101, ID, "TA", "T", DELETION_SEQUENCE_ONTOLOGY,
                             STUDY_1);
    }

    @Test
    public void singleStudySeveralNucleotidesDeletion() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1100, "TAG", "T", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1100, 1102, ID, "TAG", "T", DELETION_SEQUENCE_ONTOLOGY,
                             STUDY_1);
    }

    @Test
    public void singleStudyMultiAllelicVariant() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1100, "C", "A,T", SNP_SEQUENCE_ONTOLOGY, STUDY_1);
        expectedException.expect(IllegalArgumentException.class);
        variantConverter.process(variant);
    }

    @Test
    public void singleNucleotideInsertionInPosition1() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1, "A", "TA", INSERTION_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1, 1, ID, "A", "TA", INSERTION_SEQUENCE_ONTOLOGY,
                             STUDY_1);
    }

    @Test
    public void singleNucleotideDeletionInPosition1() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1, "AT", "T", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1, 2, ID, "AT", "T", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);
    }


    @Test
    public void severalNucleotidesInsertionInPosition1() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1, "A", "GGTA", INSERTION_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1, 1, ID, "A", "GGTA", INSERTION_SEQUENCE_ONTOLOGY,
                             STUDY_1);
    }

    @Test
    public void severalNucleotidesDeletionInPosition1() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1, "ATTG", "G", DELETION_SEQUENCE_ONTOLOGY, STUDY_1);
        VariantContext variantContext = variantConverter.process(variant);
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1, 4, ID, "ATTG", "G", DELETION_SEQUENCE_ONTOLOGY,
                             STUDY_1);
    }

    @Test
    public void twoStudiesSingleVariant() throws Exception {
        Variant variant = buildVariant(GENBANK_ACCESSION_1, 1000, "T", "G", SNP_SEQUENCE_ONTOLOGY, STUDY_1, STUDY_2);

        // process variant
        VariantContext variantContext = variantConverter.process(variant);

        // check processed variant
        assertVariantContext(variantContext, SEQUENCE_NAME_1, 1000, 1000, ID, "T", "G", SNP_SEQUENCE_ONTOLOGY, STUDY_1,
                             STUDY_2);
    }

}
