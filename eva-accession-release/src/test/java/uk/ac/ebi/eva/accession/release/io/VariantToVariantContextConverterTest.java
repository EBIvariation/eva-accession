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

package uk.ac.ebi.eva.accession.release.io;

import htsjdk.variant.variantcontext.Allele;
import htsjdk.variant.variantcontext.VariantContext;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.eva.commons.core.models.Annotation;
import uk.ac.ebi.eva.commons.core.models.ConsequenceType;
import uk.ac.ebi.eva.commons.core.models.IConsequenceType;
import uk.ac.ebi.eva.commons.core.models.VariantSource;
import uk.ac.ebi.eva.commons.core.models.factories.VariantAggregatedVcfFactory;
import uk.ac.ebi.eva.commons.core.models.factories.VariantVcfFactory;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;
import uk.ac.ebi.eva.commons.core.models.ws.VariantSourceEntryWithSampleNames;
import uk.ac.ebi.eva.commons.core.models.ws.VariantWithSamplesAndAnnotation;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class VariantToVariantContextConverterTest {

    private static final String FILE_ID = "fileId";

    private static final String SNP_SEQUENCE_ONTOLOGY = "SO:0001483";

    private static final String CHR_1 = "1";

    private static final String ID = "rs123";

    private static final String STUDY_1 = "study_1";

    private static final String STUDY_2 = "study_2";

    @Test
    public void singleStudySNV() throws Exception {
        Variant variant = buildVariant();

        VariantToVariantContextConverter variantConverter = new VariantToVariantContextConverter();
        VariantContext variantContext = variantConverter.process(variant);
        checkVariantContext(variantContext, CHR_1, 1000, 1000, ID, "C", "A");
    }

    public Variant buildVariant() {
        Variant variant = new Variant(CHR_1, 1000, 1000, "C", "A");
        variant.setMainId(ID);
        VariantSourceEntry sourceEntry = new VariantSourceEntry(STUDY_1, FILE_ID);
        sourceEntry.addAttribute("VC", SNP_SEQUENCE_ONTOLOGY);
        sourceEntry.addAttribute("SID", STUDY_1);
        variant.addSourceEntry(sourceEntry);
        return variant;
    }

    /*
        @Test
        public void singleStudySingleNucleotideInsertion() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1100", "id", "T", "TG", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1",
                          "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            checkVariantContext(variantContext, CHR_1, 1100, 1100, "T", "TG", variantSA.getSourceEntries());
        }

        @Test
        public void singleStudySeveralNucleotidesInsertion() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1100", "id", "T", "TGA", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1",
                          "1|1", "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            checkVariantContext(variantContext, CHR_1, 1100, 1100, "T", "TGA", variantSA.getSourceEntries());
        }

        @Test
        public void singleStudySingleNucleotideDeletion() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1100", "id", "TA", "T", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1",
                          "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            checkVariantContext(variantContext, CHR_1, 1100, 1101, "TA", "T", variantSA.getSourceEntries());
        }

        @Test
        public void singleStudySeveralNucleotidesDeletion() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1100", "id", "TAG", "T", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1",
                          "1|1", "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            checkVariantContext(variantContext, CHR_1, 1100, 1102, "TAG", "T", variantSA.getSourceEntries());
        }

        @Test
        public void singleStudyMultiAllelicVariant() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1000", "id", "C", "A,T", "100", "PASS", ".", "GT", "0|0", "0|2", "0|1", "1|1",
                          "1|2", "2|2");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(2, variants.size());
            VariantWithSamplesAndAnnotation variantSA1 = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);
            VariantWithSamplesAndAnnotation variantSA2 = new VariantWithSamplesAndAnnotation(variants.get(1), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variant1Context = variantConverter.process(variantSA1);
            VariantContext variant2Context = variantConverter.process(variantSA2);

            checkVariantContext(variant1Context, CHR_1, 1000, 1000, "C", "A", variantSA1.getSourceEntries());
            checkVariantContext(variant2Context, CHR_1, 1000, 1000, "C", "T", variantSA2.getSourceEntries());
        }

        @Test
        public void singleStudyMultiAllelicIndel() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1000", "id", "C", "CA,T", "100", "PASS", ".", "GT", "0|0", "0|2", "0|1", "1|1",
                          "1|2", "2|2");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(2, variants.size());
            VariantWithSamplesAndAnnotation variantSA1 = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);
            VariantWithSamplesAndAnnotation variantSA2 = new VariantWithSamplesAndAnnotation(variants.get(1), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variant1Context = variantConverter.process(variantSA1);
            VariantContext variant2Context = variantConverter.process(variantSA2);

            checkVariantContext(variant1Context, CHR_1, 1000, 1000, "C", "CA", variantSA1.getSourceEntries());
            checkVariantContext(variant2Context, CHR_1, 1000, 1000, "C", "T", variantSA2.getSourceEntries());
        }

        @Test
        public void complexVariant() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1000", "id", "TAC", "TACT,TC", "100", "PASS", ".", "GT", "0|0", "0|2", "0|1", "1|1",
                          "1|2", "2|2");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(2, variants.size());
            VariantWithSamplesAndAnnotation variantSA1 = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);
            VariantWithSamplesAndAnnotation variantSA2 = new VariantWithSamplesAndAnnotation(variants.get(1), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variant1Context = variantConverter.process(variantSA1);
            VariantContext variant2Context = variantConverter.process(variantSA2);

            checkVariantContext(variant1Context, CHR_1, 1002, 1002, "C", "CT", variantSA1.getSourceEntries());
            checkVariantContext(variant2Context, CHR_1, 1000, 1001, "TA", "T", variantSA2.getSourceEntries());
        }

        @Test
        public void singleNucleotideInsertionInPosition1() {
            // create SNV variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1", "id", "A", "TA", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1",
                          "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            checkVariantContext(variantContext, CHR_1, 1, 1, "A", "TA", variantSA.getSourceEntries());
        }

        @Test
        public void singleNucleotideDeletionInPosition1() {
            // create SNV variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1", "id", "AT", "T", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1",
                          "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            checkVariantContext(variantContext, CHR_1, 1, 2, "AT", "T",variantSA.getSourceEntries());
        }


        @Test
        public void severalNucleotidesInsertionInPosition1() {
            // create SNV variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1", "id", "A", "GGTA", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1",
                          "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            checkVariantContext(variantContext, CHR_1, 1, 1, "A", "GGTA", variantSA.getSourceEntries());
        }

        @Test
        public void severalNucleotidesDeletionInPosition1() {
            // create SNV variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String
                    .join("\t", CHR_1, "1", "id", "ATTG", "G", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1",
                          "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);
            System.out.println(variantContext);
            checkVariantContext(variantContext, CHR_1, 1, 4, "ATTG", "G", variantSA.getSourceEntries());
        }

        @Test
        public void twoStudiesNoConflictingNamesSingleVariant() {
            // create test variant, with two studies and samples with not conflicting names
            VariantWithSamplesAndAnnotation variant = new VariantWithSamplesAndAnnotation(CHR_1, 1000, 1000, "T", "G");

            // initialize study 1 metadata and genotypes
            List<String> source1SampleNames = Arrays.asList("SX_1", "SX_2", "SX_3", "SX_4");
            VariantSource
                    source1 = createTestVariantSource("study_1", "file_1", "testStudy1", "testFile1", source1SampleNames);
            VariantSourceEntry entry1 = new VariantSourceEntry("file_1", "study_1", null, "GT");
            addGenotypes(entry1, "0|0", "0|1", "0|1", "0|0");
            VariantSourceEntryWithSampleNames study1Entry =
                    new VariantSourceEntryWithSampleNames(entry1, source1SampleNames);
            variant.addSourceEntry(study1Entry);

            // initialie study 2 metadata and genotypes
            List<String> source2SampleNames = Arrays.asList("SY_1", "SY_2", "SY_3", "SY_4", "SY_5", "SY_6");
            VariantSource
                    source2 = createTestVariantSource("study_2", "file_2", "testStudy2", "testFile2", source2SampleNames);

            VariantSourceEntry entry2 = new VariantSourceEntry("file_2", "study_2", null, "GT");
            addGenotypes(entry2, "0|0", "1|0", "1|1", "1|1", "0|0", "1|0");
            VariantSourceEntryWithSampleNames study2Entry =
                    new VariantSourceEntryWithSampleNames(entry2, source2SampleNames);
            variant.addSourceEntry(study2Entry);

            // process variant
            VariantToVariantContextConverter variantConverter = new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variant);

            // check processed variant
            checkVariantContext(variantContext, CHR_1, 1000, 1000, "T", "G", variant.getSourceEntries());
        }

        private void addGenotypes(VariantSourceEntry variantSourceEntry, String ... genotypes) {
            // add the genotyeps to the variant source entry, in the same order they are in the list
            for (String genotype : genotypes) {
                Map<String, String> sampleData = new HashMap<>();
                sampleData.put("GT", genotype);
                variantSourceEntry.addSampleData(sampleData);
            }
        }

        @Test
        public void twoStudiesConflictingNamesSingleVariant() {
            // create test variant, with two studies and samples with not conflicting names
            VariantWithSamplesAndAnnotation variant = new VariantWithSamplesAndAnnotation(CHR_1, 1000, 1000, "T", "G");

            // studies and samples names
            String study1 = "study_1";
            String study2 = "study_2";
            String file1 = "file_1";
            String file2 = "file_2";
            List<String> study1SampleNames = Arrays.asList("SX_1", "SX_2", "SX_3", "SX_4");
            List<String> study2SampleNames = Arrays.asList("SX_1", "SX_2", "SX_3", "SX_4", "SX_5", "SX_6");

            // sample name corrections map (usually created by VariantExporter)
            Map<String, Map<String, String>> sampleNamesCorrections = new HashMap<>();
            Map<String, String> file1SampleNameCorrections = study1SampleNames.stream().collect(
                    Collectors.toMap(Function.identity(), s -> file1 + "_" + s));
            Map<String, String> file2SampleNameCorrections = study2SampleNames.stream().collect(
                    Collectors.toMap(Function.identity(), s -> file2 + "_" + s));
            sampleNamesCorrections.put(file1, file1SampleNameCorrections);
            sampleNamesCorrections.put(file2, file2SampleNameCorrections);

            // variant sources
            VariantSource source1 = createTestVariantSource(study1, file1, "testStudy1", "testFile1", study1SampleNames);
            VariantSourceEntry source1EntryWithoutSamples = new VariantSourceEntry(file1, study1);
            addGenotypes(source1EntryWithoutSamples, "0|0", "0|1", "0|1", "0|0");
            VariantSourceEntryWithSampleNames source1Entry = new VariantSourceEntryWithSampleNames(
                    source1EntryWithoutSamples, study1SampleNames);
            variant.addSourceEntry(source1Entry);

            VariantSource source2 = createTestVariantSource(study2, file2, "testStudy2", "testFile2", study2SampleNames);
            VariantSourceEntry source2EntryWithoutSamples = new VariantSourceEntry(file2, study2);
            addGenotypes(source2EntryWithoutSamples, "0|0", "1|0", "1|1", "-1|-1", "0|0", "1|0");
            VariantSourceEntryWithSampleNames source2Entry = new VariantSourceEntryWithSampleNames(
                    source2EntryWithoutSamples, study2SampleNames);
            variant.addSourceEntry(source2Entry);

            // process variant
            VariantToVariantContextConverter variantConverter = new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variant);

            // check processed variant
            Set<String> sampleNames = source1.getSamplesPosition().keySet().stream().map(
                    s -> source1Entry.getFileId() + "_" + s).collect(Collectors.toSet());
            sampleNames.addAll(source2.getSamplesPosition().keySet().stream().map(s -> source2Entry.getFileId() + "_" + s)
                                      .collect(Collectors.toSet()));
            checkVariantContext(variantContext, CHR_1, 1000, 1000, "T", "G", variant.getSourceEntries());
        }


        @Test
        public void csqAnnotation() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String.join(
                    "\t", CHR_1, "1000", "id", "C", "A", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1", "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            Set<IConsequenceType> consequenceTypes = new HashSet<>();
            Set<Integer> soAcc1 = new HashSet<>(Arrays.asList(1894, 1624));
            Set<Integer> soAcc2 = new HashSet<>(Collections.singletonList(1907));

            ConsequenceType consequenceType = new ConsequenceType("gene", "ensembleGeneId",
                                                                  "EnsembleTransId", "strand",
                                                                  "bioType", 10, 10, 10,
                                                                  "aaChange", "codon", null, null, soAcc1, 0);
            ConsequenceType consequenceType2 = new ConsequenceType("gene2", null,
                                                                   "EnsembleTransId2", "strand2",
                                                                   "", 20, 20, 20,
                                                                   "aaChange2", "codon2", null, null, soAcc2, 0);
            consequenceTypes.add(consequenceType);
            consequenceTypes.add(consequenceType2);
            //variants.get(0).getAnnotation().setConsequenceTypes(consequenceTypes);

            // populate variant with test csq data
            Annotation annotation = new Annotation(variantSA.getChromosome(), variantSA.getStart(), variantSA.getEnd(),
                                                   "", "", null, consequenceTypes);
            variantSA.setAnnotation(annotation);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);

            // test if CSQ is properly processed
            assertFalse(variantContext.getCommonInfo().getAttributes().isEmpty());
            String csq = (String) variantContext.getCommonInfo().getAttribute("CSQ");
            assertNotNull(csq);
            List<String> entries = Arrays.asList(csq.split(","));
            assertEquals(2, entries.size());
            assertTrue(entries.contains("A|regulatory_region_ablation&3_prime_UTR_variant|gene|ensembleGeneId|EnsembleTransId|bioType|10|10"));
            assertTrue(entries.contains("A|feature_elongation|gene2||EnsembleTransId2||20|20"));
        }

        @Test
        public void csqAnnotationWithoutSoTermsAndGeneName() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String.join(
                    "\t", CHR_1, "1000", "id", "C", "A", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1", "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);

            Set<IConsequenceType> consequenceTypes = new HashSet<>();
            Set<Integer> soAcc1 = new HashSet<>();
            Set<Integer> soAcc2 = new HashSet<>();
            ConsequenceType consequenceType = new ConsequenceType(null, "ensembleGeneId",
                                                                  "EnsembleTransId", "strand",
                                                                  "bioType", 10, 10, 10,
                                                                  "aaChange", "codon", null, null, soAcc1, 0);
            ConsequenceType consequenceType2 = new ConsequenceType("", null,
                                                                   "EnsembleTransId2", "strand2",
                                                                   "", 20, 20, 20,
                                                                   "aaChange2", "codon2", null, null, soAcc2, 0);
            consequenceTypes.add(consequenceType);
            consequenceTypes.add(consequenceType2);

            // populate variant with test csq data
            Annotation annotation = new Annotation(variantSA.getChromosome(), variantSA.getStart(), variantSA.getEnd(),
                                                   "", "", null, consequenceTypes);
    //        variantSA.getAnnotation().setAlternativeAllele("A");
            variantSA.setAnnotation(annotation);

            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);

            // test if CSQ is properly processed
            assertFalse(variantContext.getCommonInfo().getAttributes().isEmpty());
            String csq = (String) variantContext.getCommonInfo().getAttribute("CSQ");
            assertNotNull(csq);
            List<String> entries = Arrays.asList(csq.split(","));
            assertEquals(2, entries.size());
            assertTrue(entries.contains("A|||ensembleGeneId|EnsembleTransId|bioType|10|10"));
            assertTrue(entries.contains("A||||EnsembleTransId2||20|20"));
        }

        @Test
        public void testNoCsqAnnotationCreatedFromVariantWithoutInfo() {
            // create variant
            VariantSource variantSource = createTestVariantSource(STUDY_1);
            String variantLine = String.join(
                    "\t", CHR_1, "1000", "id", "C", "A", "100", "PASS", ".", "GT", "0|0", "0|0", "0|1", "1|1", "1|1", "0|1");
            List<Variant> variants = variantFactory.create(CHR_1, STUDY_1 , variantLine);
            assertEquals(1, variants.size());
            VariantWithSamplesAndAnnotation variantSA = new VariantWithSamplesAndAnnotation(variants.get(0), s1s6SampleList);


            // export variant
            VariantToVariantContextConverter variantConverter =
                    new VariantToVariantContextConverter();
            VariantContext variantContext = variantConverter.process(variantSA);

            // test if CSQ is not present
            assertTrue(variantContext.getCommonInfo().getAttributes().isEmpty());
            String csq = (String) variantContext.getCommonInfo().getAttribute("CSQ");
            assertNull(csq);
        }
    */
    private void checkVariantContext(VariantContext variantContext, String chromosome, int start, int end, String id,
                                     String ref, String alt) {
        assertEquals(chromosome, variantContext.getContig());
        assertEquals(start, variantContext.getStart());
        assertEquals(end, variantContext.getEnd());
        assertEquals(Allele.create(ref, true), variantContext.getReference());
        assertEquals(Collections.singletonList(Allele.create(alt, false)), variantContext.getAlternateAlleles());
        assertEquals(id, variantContext.getID());
        assertTrue(variantContext.getFilters().isEmpty());
        assertEquals(2, variantContext.getCommonInfo().getAttributes().size());
        assertEquals(0, variantContext.getSampleNames().size());
    }

    private VariantSource createTestVariantSource(String studyId) {
        return createTestVariantSource(studyId, FILE_ID, "studyName", "name", Collections.emptyList());
    }

    private VariantSource createTestVariantSource(String studyId, String fileId, String studyName, String fileName, List<String> sampleList) {
        Map<String, Integer> samplesPosition = new HashMap<>();
        int index = sampleList.size();
        for (String s : sampleList) {
            samplesPosition.put(s, index++);
        }
        return new VariantSource(fileId, fileName, studyId, studyName, null, null, null, samplesPosition, null, null);
    }

}
