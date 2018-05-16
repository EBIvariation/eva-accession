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
package uk.ac.ebi.eva.accession.pipeline.io;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.springframework.batch.item.ExecutionContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.pipeline.test.MongoTestConfiguration;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@DataJpaTest
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class AccessionWriterTest {

    private static final int TAXONOMY = 3880;

    private static final long EXPECTED_ACCESSION = 10000000000L;

    private static final int START_1 = 100;

    private static final int START_2 = 200;

    private static final String ALTERNATE_ALLELE = "T";

    private static final String REFERENCE_ALLELE = "A";

    private static final int ACCESSION_COLUMN = 2;

    private static final String ACCESSION_PREFIX = "ss";

    @Autowired
    private SubmittedVariantAccessioningService service;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private AccessionWriter accessionWriter;

    private File output;

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        Path fastaPath = Paths.get(AccessionReportWriterTest.class.getResource("/input-files/fasta/mock.fa").getFile());
        AccessionReportWriter accessionReportWriter = new AccessionReportWriter(output,
                                                                                new FastaSequenceReader(fastaPath));
        accessionWriter = new AccessionWriter(service, accessionReportWriter);
        accessionReportWriter.open(new ExecutionContext());
    }

    @Test
    @DirtiesContext
    public void saveSingleAccession() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1, "reference",
                                                        "alternate", false);

        accessionWriter.write(Collections.singletonList(variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getAccessions(
                Collections.singletonList(variant));
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.iterator().next().getAccession());

        assertVariantEquals(variant, accessions.iterator().next().getData());
    }

    private void assertVariantEquals(ISubmittedVariant expectedvariant, ISubmittedVariant actualVariant) {
        assertEquals(expectedvariant.getAssemblyAccession(), actualVariant.getAssemblyAccession());
        assertEquals(expectedvariant.getTaxonomyAccession(), actualVariant.getTaxonomyAccession());
        assertEquals(expectedvariant.getProjectAccession(), actualVariant.getProjectAccession());
        assertEquals(expectedvariant.getContig(), actualVariant.getContig());
        assertEquals(expectedvariant.getStart(), actualVariant.getStart());
        assertEquals(expectedvariant.getReferenceAllele(), actualVariant.getReferenceAllele());
        assertEquals(expectedvariant.getAlternateAllele(), actualVariant.getAlternateAllele());
        assertEquals(expectedvariant.isSupportedByEvidence(), actualVariant.isSupportedByEvidence());
    }

    @Test
    @DirtiesContext
    public void saveTwoAccession() throws Exception {
        SubmittedVariant firstVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                             "reference", "alternate", false);
        SubmittedVariant secondVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_2,
                                                              "reference", "alternate", false);

        accessionWriter.write(Arrays.asList(firstVariant, secondVariant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getAccessions(
                Arrays.asList(firstVariant, secondVariant));
        assertEquals(2, accessions.size());

        Iterator<AccessionWrapper<ISubmittedVariant, String, Long>> iterator = accessions.iterator();
        ISubmittedVariant firstSavedVariant = iterator.next().getData();
        ISubmittedVariant secondSavedVariant = iterator.next().getData();
        if (firstSavedVariant.getStart() == firstVariant.getStart()) {
            assertVariantEquals(firstVariant, firstSavedVariant);
            assertVariantEquals(secondVariant, secondSavedVariant);
        } else {
            assertVariantEquals(secondVariant, firstSavedVariant);
            assertVariantEquals(firstVariant, secondSavedVariant);
        }
    }

    @Test
    @DirtiesContext
    public void saveSameAccessionTwice() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1, "reference",
                                                        "alternate", false);

        accessionWriter.write(Arrays.asList(variant, variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getAccessions(
                Collections.singletonList(variant));
        assertEquals(1, accessions.size());

        assertVariantEquals(variant, accessions.iterator().next().getData());
    }

    @Test
    @DirtiesContext
    public void testSaveInitializesCreatedDate() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", "contig", START_1,
                                                        "reference", "alternate", false);
        LocalDateTime beforeSave = LocalDateTime.now();
        accessionWriter.write(Collections.singletonList(variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getAccessions(
                Collections.singletonList(variant));
        assertEquals(1, accessions.size());
        ISubmittedVariant savedVariant = accessions.iterator().next().getData();
        assertTrue(beforeSave.isBefore(savedVariant.getCreatedDate()));
    }

    @Test
    @DirtiesContext
    public void createAccessionAndItAppearsInTheReportVcf() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                        REFERENCE_ALLELE, ALTERNATE_ALLELE, false);

        accessionWriter.write(Arrays.asList(variant, variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.getAccessions(
                Collections.singletonList(variant));
        assertEquals(1, accessions.size());

        String vcfLine = AccessionReportWriterTest.getFirstVariantLine(output);
        assertEquals(vcfLine.split("\t")[ACCESSION_COLUMN],
                     ACCESSION_PREFIX + accessions.iterator().next().getAccession());
    }

    @Test
    public void shouldThrowIfSomeVariantsWereNotAccessioned() {
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                        REFERENCE_ALLELE, ALTERNATE_ALLELE, false);

        thrown.expect(IllegalStateException.class);
        accessionWriter.assertCountsMatch(Collections.singletonList(variant), new ArrayList<>());
    }

    @Test
    public void shouldThrowIfSomeVariantsWereNotAccessionedInAChunkWithRepeatedVariants() {
        SubmittedVariant firstVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                             "reference", "alternate", false);
        SubmittedVariant secondVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_2,
                                                              "reference", "alternate", false);
        List<SubmittedVariant> variants = Arrays.asList(firstVariant, secondVariant, firstVariant, secondVariant);

        ArrayList<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = new ArrayList<>();
        accessions.add(new AccessionWrapper<>(EXPECTED_ACCESSION, "hashedMessage", secondVariant));

        thrown.expect(IllegalStateException.class);
        accessionWriter.assertCountsMatch(variants, accessions);
    }
}
