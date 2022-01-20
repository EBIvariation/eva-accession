/*
 * Copyright 2018 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.pipeline.batch.io;

import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.service.GetOrCreateAccessionWrapperCreator;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.nonhuman.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.contig.ContigNaming;
import uk.ac.ebi.eva.accession.core.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.pipeline.batch.processors.VariantConverter;
import uk.ac.ebi.eva.accession.pipeline.configuration.InputParametersConfiguration;
import uk.ac.ebi.eva.accession.pipeline.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.pipeline.metric.AccessioningMetric;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;
import uk.ac.ebi.eva.commons.core.models.pipeline.VariantSourceEntry;
import uk.ac.ebi.eva.metrics.metric.MetricCompute;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static uk.ac.ebi.eva.accession.pipeline.batch.processors.ContigToGenbankReplacerProcessor.ORIGINAL_CHROMOSOME;

@RunWith(SpringRunner.class)
@EnableAutoConfiguration
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, ListenersConfiguration.class, InputParametersConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class AccessionWriterTest {

    private static final int TAXONOMY = 3880;

    private static final long EXPECTED_ACCESSION = 5000000000L;

    private static final String CONTIG_1 = "genbank_1";

    private static final String CHROMOSOME_1 = "chr1";

    private static final String CONTIG_2 = "genbank_2";

    private static final String CHROMOSOME_2 = "chr2";

    private static final String CONTIG_3 = "genbank_3";

    private static final String CHROMOSOME_3 = "chr3";

    private static final int START_1 = 2;

    private static final int START_2 = 200;

    private static final String ALTERNATE_ALLELE = "T";

    private static final String REFERENCE_ALLELE = "A";

    private static final int ACCESSION_COLUMN = 2;

    private static final String ACCESSION_PREFIX = "ss";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Long NONNULL_CLUSTERED_VARIANT = 5L;

    private static final Boolean SUPPORTED_BY_EVIDENCE = true;

    private static final Boolean MATCHES_ASSEMBLY = true;

    private static final Boolean ALLELES_MATCH = false;

    private static final Boolean VALIDATED = false;

    private static final String REFERENCE = "reference";

    private static final String ALTERNATE = "alternate";

    @Autowired
    private SubmittedVariantAccessioningService service;

    @Autowired
    private MongoTemplate mongoTemplate;

    @Autowired
    private MetricCompute metricCompute;

    private ContigMapping contigMapping;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private AccessionWriter accessionWriter;

    private File output;

    private File variantOutput;

    private VariantConverter variantConverter;

    @Before
    public void setUp() throws Exception {
        contigMapping = new ContigMapping(Arrays.asList(
                new ContigSynonyms(CHROMOSOME_1, "assembled-molecule", "1", CONTIG_1, "refseq_1", "chr1", true),
                new ContigSynonyms(CHROMOSOME_2, "assembled-molecule", "2", CONTIG_2, "refseq_2", "chr2", true)));
        output = temporaryFolderRule.newFile();
        variantOutput = new File(output.getAbsolutePath() + AccessionReportWriter.VARIANTS_FILE_SUFFIX);
        Path fastaPath = Paths.get(AccessionReportWriterTest.class.getResource("/input-files/fasta/mock.fa").toURI());
        AccessionReportWriter accessionReportWriter = new AccessionReportWriter(output,
                                                                                new FastaSequenceReader(fastaPath),
                                                                                contigMapping,
                                                                                ContigNaming.SEQUENCE_NAME);
        variantConverter = new VariantConverter("assembly", TAXONOMY, "project");
        accessionWriter = new AccessionWriter(service, accessionReportWriter, variantConverter, metricCompute);
        accessionReportWriter.open(new ExecutionContext());
        mongoTemplate.dropCollection(SubmittedVariantEntity.class);
    }

    @After
    public void tearDown(){
        metricCompute.clearCount();
    }

    @Test
    @DirtiesContext
    public void saveSingleAccession() throws Exception {
        Variant variant = buildMockVariant("contig", START_1);

        accessionWriter.write(Collections.singletonList(variant));

        ISubmittedVariant submittedVariant = variantConverter.convert(variant);
        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service
                .get(Collections.singletonList(submittedVariant));
        assertEquals(1, accessions.size());
        assertEquals(1, metricCompute.getCount(AccessioningMetric.ACCESSIONED_VARIANTS));
        assertEquals(EXPECTED_ACCESSION, (long) accessions.iterator().next().getAccession());

        assertEquals(submittedVariant, accessions.iterator().next().getData());
        // the creation date is added by the writer
        assertNotNull(accessions.iterator().next().getData().getCreatedDate());
    }

    @Test
    @DirtiesContext
    public void saveTwoAccession() throws Exception {
        Variant firstVariant = buildMockVariant("contig", START_1);
        Variant secondVariant = buildMockVariant("contig", START_2);

        List<Variant> variants = Arrays.asList(firstVariant, secondVariant);
        accessionWriter.write(variants);

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(convert(variants));
        assertEquals(2, accessions.size());
        assertEquals(2, metricCompute.getCount(AccessioningMetric.ACCESSIONED_VARIANTS));

        Iterator<AccessionWrapper<ISubmittedVariant, String, Long>> iterator = accessions.iterator();
        ISubmittedVariant firstSavedVariant = iterator.next().getData();
        ISubmittedVariant secondSavedVariant = iterator.next().getData();
        if (firstSavedVariant.getStart() == firstVariant.getStart()) {
            assertEquals(variantConverter.convert(firstVariant), firstSavedVariant);
            assertEquals(variantConverter.convert(secondVariant), secondSavedVariant);
        } else {
            assertEquals(variantConverter.convert(secondVariant), firstSavedVariant);
            assertEquals(variantConverter.convert(firstVariant), secondSavedVariant);
        }
    }

    private List<ISubmittedVariant> convert(List<Variant> variants) {
        return variants.stream().map(variantConverter::convert).collect(Collectors.toList());
    }

    @Test
    @DirtiesContext
    public void variantInsertionCheckOrder() throws Exception {
        Variant firstVariant = buildMockVariant(CONTIG_1, START_1, "C", "A");
        Variant secondVariant = buildMockVariant(CONTIG_1, START_1, "", "A");

        List<Variant> variants = Arrays.asList(firstVariant, secondVariant);
        accessionWriter.write(variants);

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(convert(variants));
        assertEquals(2, accessions.size());

        int firstVariantLineNumber = getVariantLineNumberByPosition(variantOutput, CHROMOSOME_1 + "\t" + START_1);
        //secondVariant position is START_1 - 1 because it is an insertion and the context base is added
        int secondVariantLineNumber = getVariantLineNumberByPosition(variantOutput,
                                                                     CHROMOSOME_1 + "\t" + (START_1 - 1));
        assertTrue(firstVariantLineNumber > secondVariantLineNumber);
    }

    private static int getVariantLineNumberByPosition(File output, String position) throws IOException {
        BufferedReader fileInputStream = new BufferedReader(new InputStreamReader(new FileInputStream(output)));
        String line;
        int lineNumber = 0;
        while ((line = fileInputStream.readLine()) != null) {
            if (line.startsWith(position)) {
                return lineNumber;
            }
            lineNumber++;
        }
        throw new IllegalStateException("The VCF does not contain any variant with position " + position);
    }

    @Test
    @DirtiesContext
    public void saveSameAccessionTwice() throws Exception {
        Variant variant = buildMockVariant("contig", START_1);

        accessionWriter.write(Arrays.asList(variant, variant));

        ISubmittedVariant submittedVariant = variantConverter.convert(variant);
        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service
                .get(Collections.singletonList(submittedVariant));
        assertEquals(1, accessions.size());
        assertEquals(1, metricCompute.getCount(AccessioningMetric.ACCESSIONED_VARIANTS));
        assertEquals(1, metricCompute.getCount(AccessioningMetric.DISTINCT_VARIANTS));
        assertEquals(1, metricCompute.getCount(AccessioningMetric.DUPLICATE_VARIANTS));

        assertEquals(submittedVariant, accessions.iterator().next().getData());
    }

    @Test
    @DirtiesContext
    public void testSaveInitializesCreatedDate() throws Exception {
        Variant variant = buildMockVariant("contig", START_1);
        LocalDateTime beforeSave = LocalDateTime.now();
        accessionWriter.write(Collections.singletonList(variant));
        LocalDateTime afterSave = LocalDateTime.now();

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service
                .get(Collections.singletonList(variantConverter.convert(variant)));
        assertEquals(1, accessions.size());
        ISubmittedVariant savedVariant = accessions.iterator().next().getData();
        assertTrue(beforeSave.isBefore(savedVariant.getCreatedDate()));
        assertTrue(afterSave.isAfter(savedVariant.getCreatedDate()));
    }

    @Test
    @DirtiesContext
    public void createAccessionAndItAppearsInTheReportVcf() throws Exception {
        Variant variant = buildMockVariant("contig", START_1);

        accessionWriter.write(Arrays.asList(variant, variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service
                .get(Collections.singletonList(variantConverter.convert(variant)));
        assertEquals(1, accessions.size());

        String vcfLine = AccessionReportWriterTest.getFirstVariantLine(variantOutput);
        assertEquals(vcfLine.split("\t")[ACCESSION_COLUMN],
                     ACCESSION_PREFIX + accessions.iterator().next().getAccession());
    }

    @Test
    public void shouldThrowIfSomeVariantsWereNotAccessioned() {
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                        REFERENCE_ALLELE, ALTERNATE_ALLELE, CLUSTERED_VARIANT, false,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        thrown.expect(IllegalStateException.class);
        accessionWriter.checkCountsMatch(Collections.singletonList(variant), new ArrayList<>());
    }

    @Test
    public void shouldThrowIfSomeVariantsWereNotAccessionedInAChunkWithRepeatedVariants() {
        SubmittedVariant firstVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                             REFERENCE, ALTERNATE, CLUSTERED_VARIANT, false,
                                                             MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant secondVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_2,
                                                              REFERENCE, ALTERNATE, CLUSTERED_VARIANT, false,
                                                              MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        List<SubmittedVariant> variants = Arrays.asList(firstVariant, secondVariant, firstVariant, secondVariant);

        ArrayList<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = new ArrayList<>();
        accessions.add(new AccessionWrapper<>(EXPECTED_ACCESSION, "hashedMessage", secondVariant));

        thrown.expect(IllegalStateException.class);
        accessionWriter.checkCountsMatch(variants,
                                         GetOrCreateAccessionWrapperCreator.convertToGetOrCreateAccessionWrapper(
                                                 accessions));
    }

    @Test
    public void shouldNotThrowIfVariantsDifferOnlyByClusteredVariantAccession() {
        SubmittedVariant variant = getSubmittedVariantWithClusteredVariant(CLUSTERED_VARIANT);
        SubmittedVariant variantWithRs = getSubmittedVariantWithClusteredVariant(NONNULL_CLUSTERED_VARIANT);
        List<SubmittedVariant> variants = Arrays.asList(variant, variant);
        AccessionWrapper<ISubmittedVariant, String, Long> accession = new AccessionWrapper<>(EXPECTED_ACCESSION,
                                                                                             "hashedMessage",
                                                                                             variantWithRs);
        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = Collections.singletonList(accession);

        accessionWriter.checkCountsMatch(variants,
                                         GetOrCreateAccessionWrapperCreator.convertToGetOrCreateAccessionWrapper(
                                                 accessions));
    }

    @Test
    public void shouldSortReport() throws Exception {
        // given
        List<Variant> variants = Arrays.asList(buildMockVariant(CONTIG_2, CHROMOSOME_2, START_1),
                                               buildMockVariant(CONTIG_1, CHROMOSOME_1, START_2),
                                               buildMockVariant(CONTIG_3, CHROMOSOME_3, START_1),
                                               buildMockVariant(CONTIG_1, CHROMOSOME_1, START_1),
                                               buildMockVariant(CONTIG_2, CHROMOSOME_2, START_2));

        // when
        accessionWriter.write(variants);

        // then
        BufferedReader fileInputStream = new BufferedReader(new InputStreamReader(new FileInputStream(variantOutput)));
        String line;
        while ((line = fileInputStream.readLine()) != null && line.startsWith("#")) {
        }

        assertThat(line, Matchers.startsWith(CHROMOSOME_2 + "\t" + START_1));
        line = fileInputStream.readLine();
        assertThat(line, Matchers.startsWith(CHROMOSOME_2 + "\t" + START_2));
        line = fileInputStream.readLine();
        assertThat(line, Matchers.startsWith(CHROMOSOME_1 + "\t" + START_1));
        line = fileInputStream.readLine();
        assertThat(line, Matchers.startsWith(CHROMOSOME_1 + "\t" + START_2));
        line = fileInputStream.readLine();
        assertThat(line, Matchers.startsWith(CHROMOSOME_3 + "\t" + START_1));
    }

    private SubmittedVariant getSubmittedVariantWithClusteredVariant(Long clusteredVariant) {
        return new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                             REFERENCE, ALTERNATE, clusteredVariant, false,
                             MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
    }

    private Variant buildMockVariant(String contig, int start) {
        return buildMockVariant(contig, contig, start, REFERENCE, ALTERNATE);
    }

    private Variant buildMockVariant(String contig, String originalChromosome, int start) {
        return buildMockVariant(contig, originalChromosome, start, REFERENCE, ALTERNATE);
    }

    private Variant buildMockVariant(String contig, int start, String reference, String alternate) {
        return buildMockVariant(contig, contig, start, reference, alternate);
    }

    private Variant buildMockVariant(String contig, String originalChromosome, int start, String reference,
                                     String alternate) {
        Variant variant = new Variant(contig, start, start, reference, alternate);
        VariantSourceEntry sourceEntry = new VariantSourceEntry("fileId", "studyId");
        sourceEntry.addAttribute(ORIGINAL_CHROMOSOME, originalChromosome);
        variant.addSourceEntry(sourceEntry);
        return variant;
    }
}
