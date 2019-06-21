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

import org.hamcrest.Matchers;
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
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.configuration.SubmittedVariantAccessioningConfiguration;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.pipeline.test.MongoTestConfiguration;

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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@DataJpaTest
@ContextConfiguration(classes = {SubmittedVariantAccessioningConfiguration.class, MongoTestConfiguration.class})
@TestPropertySource("classpath:accession-pipeline-test.properties")
public class AccessionWriterTest {

    private static final int TAXONOMY = 3880;

    private static final long EXPECTED_ACCESSION = 5000000000L;

    private static final String CONTIG_1 = "contig_1";

    private static final String CONTIG_2 = "contig_2";

    private static final int START_1 = 2;

    private static final int START_2 = 200;

    private static final String ALTERNATE_ALLELE = "T";

    private static final String REFERENCE_ALLELE = "A";

    private static final int ACCESSION_COLUMN = 2;

    private static final String ACCESSION_PREFIX = "ss";

    private static final Long CLUSTERED_VARIANT = null;

    private static final Boolean SUPPORTED_BY_EVIDENCE = true;

    private static final Boolean MATCHES_ASSEMBLY = true;

    private static final Boolean ALLELES_MATCH = false;

    private static final Boolean VALIDATED = false;

    @Autowired
    private SubmittedVariantAccessioningService service;

    @Autowired
    private ContigMapping contigMapping;

    @Rule
    public TemporaryFolder temporaryFolderRule = new TemporaryFolder();

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private AccessionWriter accessionWriter;

    private File output;

    @Before
    public void setUp() throws Exception {
        output = temporaryFolderRule.newFile();
        Path fastaPath = Paths.get(AccessionReportWriterTest.class.getResource("/input-files/fasta/mock.fa").toURI());
        AccessionReportWriter accessionReportWriter = new AccessionReportWriter(output,
                                                                                new FastaSequenceReader(fastaPath),
                                                                                contigMapping);
        accessionWriter = new AccessionWriter(service, accessionReportWriter);
        accessionReportWriter.open(new ExecutionContext());
    }

    @Test
    @DirtiesContext
    public void saveSingleAccession() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1, "reference",
                                                        "alternate", CLUSTERED_VARIANT, SUPPORTED_BY_EVIDENCE,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        accessionWriter.write(Collections.singletonList(variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(Collections.singletonList(variant));
        assertEquals(1, accessions.size());
        assertEquals(EXPECTED_ACCESSION, (long) accessions.iterator().next().getAccession());

        assertEquals(variant, accessions.iterator().next().getData());
        // the creation date is added by the writer
        assertNotNull(accessions.iterator().next().getData().getCreatedDate());
    }

    @Test
    @DirtiesContext
    public void saveTwoAccession() throws Exception {
        SubmittedVariant firstVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                             "reference", "alternate", CLUSTERED_VARIANT, false,
                                                             MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant secondVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_2,
                                                              "reference", "alternate", CLUSTERED_VARIANT, false,
                                                              MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        accessionWriter.write(Arrays.asList(firstVariant, secondVariant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = 
                service.get(Arrays.asList(firstVariant, secondVariant));
        assertEquals(2, accessions.size());

        Iterator<AccessionWrapper<ISubmittedVariant, String, Long>> iterator = accessions.iterator();
        ISubmittedVariant firstSavedVariant = iterator.next().getData();
        ISubmittedVariant secondSavedVariant = iterator.next().getData();
        if (firstSavedVariant.getStart() == firstVariant.getStart()) {
            assertEquals(firstVariant, firstSavedVariant);
            assertEquals(secondVariant, secondSavedVariant);
        } else {
            assertEquals(secondVariant, firstSavedVariant);
            assertEquals(firstVariant, secondSavedVariant);
        }
    }

    @Test
    @DirtiesContext
    public void variantInsertionCheckOrder() throws Exception {
        SubmittedVariant firstVariant = new SubmittedVariant("assembly", TAXONOMY, "project", CONTIG_1, START_1,
                                                             "C", "A", CLUSTERED_VARIANT, false, MATCHES_ASSEMBLY,
                                                             ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant secondVariant = new SubmittedVariant("assembly", TAXONOMY, "project", CONTIG_1, START_1,
                                                              "", "A", CLUSTERED_VARIANT, false, MATCHES_ASSEMBLY,
                                                              ALLELES_MATCH, VALIDATED, null);

        accessionWriter.write(Arrays.asList(firstVariant, secondVariant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions =
                service.get(Arrays.asList(firstVariant, secondVariant));
        assertEquals(2, accessions.size());

        int firstVariantLineNumber = getVariantLineNumberByPosition(output, CONTIG_1 + "\t" + START_1);
        //secondVariant position is 1 because it is an insertion and the context base is added
        int secondVariantLineNumber = getVariantLineNumberByPosition(output, CONTIG_1 + "\t" + (START_1 - 1));
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
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1, "reference",
                                                        "alternate", CLUSTERED_VARIANT, false, MATCHES_ASSEMBLY,
                                                        ALLELES_MATCH, VALIDATED, null);

        accessionWriter.write(Arrays.asList(variant, variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(Collections.singletonList(variant));
        assertEquals(1, accessions.size());

        assertEquals(variant, accessions.iterator().next().getData());
    }

    @Test
    @DirtiesContext
    public void testSaveInitializesCreatedDate() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("accession", TAXONOMY, "project", "contig", START_1,
                                                        "reference", "alternate", CLUSTERED_VARIANT, false,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        LocalDateTime beforeSave = LocalDateTime.now();
        accessionWriter.write(Collections.singletonList(variant));
        LocalDateTime afterSave = LocalDateTime.now();

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(Collections.singletonList(variant));
        assertEquals(1, accessions.size());
        ISubmittedVariant savedVariant = accessions.iterator().next().getData();
        assertTrue(beforeSave.isBefore(savedVariant.getCreatedDate()));
        assertTrue(afterSave.isAfter(savedVariant.getCreatedDate()));
    }

    @Test
    @DirtiesContext
    public void createAccessionAndItAppearsInTheReportVcf() throws Exception {
        SubmittedVariant variant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_1,
                                                        REFERENCE_ALLELE, ALTERNATE_ALLELE, CLUSTERED_VARIANT, false,
                                                        MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);

        accessionWriter.write(Arrays.asList(variant, variant));

        List<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = service.get(Collections.singletonList(variant));
        assertEquals(1, accessions.size());

        String vcfLine = AccessionReportWriterTest.getFirstVariantLine(output);
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
                                                             "reference", "alternate", CLUSTERED_VARIANT, false,
                                                             MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant secondVariant = new SubmittedVariant("assembly", TAXONOMY, "project", "contig", START_2,
                                                              "reference", "alternate", CLUSTERED_VARIANT, false,
                                                              MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        List<SubmittedVariant> variants = Arrays.asList(firstVariant, secondVariant, firstVariant, secondVariant);

        ArrayList<AccessionWrapper<ISubmittedVariant, String, Long>> accessions = new ArrayList<>();
        accessions.add(new AccessionWrapper<>(EXPECTED_ACCESSION, "hashedMessage", secondVariant));

        thrown.expect(IllegalStateException.class);
        accessionWriter.checkCountsMatch(variants, accessions);
    }

    @Test
    public void shouldSortReport() throws Exception {
        // given
        SubmittedVariant firstVariant = new SubmittedVariant("assembly", TAXONOMY, "project", CONTIG_1, START_1,
                                                             "reference", "alternate", CLUSTERED_VARIANT, false,
                                                             MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant secondVariant = new SubmittedVariant("assembly", TAXONOMY, "project", CONTIG_2, START_2,
                                                              "reference", "alternate", CLUSTERED_VARIANT, false,
                                                              MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant thirdVariant = new SubmittedVariant("assembly", TAXONOMY, "project", CONTIG_1, START_2,
                                                             "reference", "alternate", CLUSTERED_VARIANT, false,
                                                             MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        SubmittedVariant fourthVariant = new SubmittedVariant("assembly", TAXONOMY, "project", CONTIG_2, START_1,
                                                              "reference", "alternate", CLUSTERED_VARIANT, false,
                                                              MATCHES_ASSEMBLY, ALLELES_MATCH, VALIDATED, null);
        List<SubmittedVariant> variants = Arrays.asList(firstVariant, secondVariant, thirdVariant, fourthVariant);

        // when
        accessionWriter.write(variants);

        // then
        BufferedReader fileInputStream = new BufferedReader(new InputStreamReader(new FileInputStream(output)));
        String line;
        while ((line = fileInputStream.readLine()) != null && line.startsWith("#")) {
        }

        assertThat(line, Matchers.startsWith(CONTIG_1 + "\t" + START_1));
        line = fileInputStream.readLine();
        assertThat(line, Matchers.startsWith(CONTIG_1 + "\t" + START_2));
        line = fileInputStream.readLine();
        assertThat(line, Matchers.startsWith(CONTIG_2 + "\t" + START_1));
        line = fileInputStream.readLine();
        assertThat(line, Matchers.startsWith(CONTIG_2 + "\t" + START_2));
    }
}
