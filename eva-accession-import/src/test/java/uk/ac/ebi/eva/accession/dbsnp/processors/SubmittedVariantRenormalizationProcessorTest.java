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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.io.FastaSynonymSequenceReader;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ASSEMBLY_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;

public class SubmittedVariantRenormalizationProcessorTest {

    private static final String ASSEMBLY = "assembly";

    private static final int TAXONOMY = 9999;

    private static final String PROJECT = "Project1";

    private static final Long RS_ID = 12345L;

    private static final Long SS_ID = 12345L;

    private static final String HASH = "hash";

    private static final String CONTIG = "22";

    private static FastaSynonymSequenceReader fastaSynonymSequenceReader;

    private static SubmittedVariantRenormalizationProcessor renormalizer;

    @BeforeClass
    public static void setUpClass() throws Exception {
        Path fastaPath = Paths.get("src/test/resources/input-files/fasta/Gallus_gallus-5.0.test.fa");
        ContigMapping contigMapping = new ContigMapping(Collections.singletonList(new ContigSynonyms(CONTIG,
                                                                                                     "", "", "", "", "",
                                                                                                     true)));
        fastaSynonymSequenceReader = new FastaSynonymSequenceReader(contigMapping, fastaPath);
        renormalizer = new SubmittedVariantRenormalizationProcessor(fastaSynonymSequenceReader);
    }

    @AfterClass
    public static void tearDownClass() throws Exception {
        fastaSynonymSequenceReader.close();
    }

    @Test
    public void nonAmbiguousReplacements() throws Exception {
        assertNonAmbiguousDoesNotChange(3, "C", "T");   // 3:C>T
        assertNonAmbiguousDoesNotChange(3, "CG", "GC"); // 2:GCG>GGC
    }

    private void assertNonAmbiguousDoesNotChange(int position, String reference, String alternate) {
        assertMatchesExpected(position, reference, alternate, position, reference, alternate, false);
    }

    private void assertAmbiguousVariantIsRenormalized(int position, String reference, String alternate,
                                                      int renormalizedVariantExpectedStart,
                                                      String renormalizedVariantExpectedReference,
                                                      String renormalizedVariantExpectedAlternate) {

        assertMatchesExpected(position, reference, alternate, renormalizedVariantExpectedStart,
                              renormalizedVariantExpectedReference, renormalizedVariantExpectedAlternate, true);
    }

    private void assertMatchesExpected(int position, String reference, String alternate,
                                       int expectedStartAfterRenormalization,
                                       String expectedReferenceAfterRenormalization,
                                       String expectedAlternateAfterRenormalization,
                                       boolean hashShouldHaveBeenRecalculated) {
        DbsnpSubmittedVariantEntity variant = new DbsnpSubmittedVariantEntity(SS_ID, HASH, ASSEMBLY, TAXONOMY, PROJECT,
                                                                              CONTIG, position, reference, alternate,
                                                                              RS_ID, DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                              DEFAULT_ASSEMBLY_MATCH,
                                                                              DEFAULT_ALLELES_MATCH,
                                                                              DEFAULT_VALIDATED, 1);
        List<DbsnpSubmittedVariantEntity> renormalized = renormalizer.process(Collections.singletonList(variant));
        assertNotNull(renormalized);
        assertEquals(1, renormalized.size());
        assertEquals(expectedStartAfterRenormalization, renormalized.get(0).getStart());
        assertEquals(expectedReferenceAfterRenormalization, renormalized.get(0).getReferenceAllele());
        assertEquals(expectedAlternateAfterRenormalization, renormalized.get(0).getAlternateAllele());
        boolean hashHasBeenRecalculated = !variant.getHashedMessage().equals(renormalized.get(0).getHashedMessage());
        assertEquals(hashShouldHaveBeenRecalculated, hashHasBeenRecalculated);
    }

    @Test
    public void nonAmbiguousInsertions() throws Exception {
        assertNonAmbiguousDoesNotChange(3, "", "A");    // 2:G>GA
        assertNonAmbiguousDoesNotChange(3, "", "AA");   // 2:G>GAA
        assertNonAmbiguousDoesNotChange(3, "", "AAA");  // 2:G>GAAA
        assertNonAmbiguousDoesNotChange(3, "", "C");    // 2:G>GC
        assertNonAmbiguousDoesNotChange(3, "", "CGC");  // 2:G>GCGC
        assertNonAmbiguousDoesNotChange(5, "", "CGC");  // 4:G>GCGC
        assertNonAmbiguousDoesNotChange(5, "", "CC");   // 4:G>GCC
    }

    @Test
    public void nonAmbiguousDeletions() throws Exception {
        assertNonAmbiguousDoesNotChange(3, "C", "");    // 2:GC>G
        assertNonAmbiguousDoesNotChange(3, "CGC", "");  // 2:GCGC>G
        assertNonAmbiguousDoesNotChange(5, "CGC", "");  // 4:GCGC>G
        assertNonAmbiguousDoesNotChange(5, "CC", "");   // 4:GCC>G
    }

    @Test
    public void ambiguousInsertions() throws Exception {
        assertAmbiguousVariantIsRenormalized(3, "", "G", 2, "", "G");   // 2:G>GG
        assertAmbiguousVariantIsRenormalized(3, "", "CG", 2, "", "GC"); // 2:G>GCG
        assertAmbiguousVariantIsRenormalized(5, "", "CG", 4, "", "GC"); // 4:G>GCG
        assertAmbiguousVariantIsRenormalized(7, "", "C", 6, "", "C");   // 6:C>CC
        assertAmbiguousVariantIsRenormalized(7, "", "CC", 6, "", "CC"); // 6:C>CCC
        assertAmbiguousVariantIsRenormalized(7, "", "CCC", 6, "", "CCC");   // 6:C>CCCC
    }

    @Test
    public void ambiguousDeletions() throws Exception {
        assertAmbiguousVariantIsRenormalized(3, "CG", "", 2, "GC", ""); // 2:GCG>G
        assertAmbiguousVariantIsRenormalized(5, "CG", "", 4, "GC", ""); // 4:GCG>G
        assertAmbiguousVariantIsRenormalized(6, "C", "", 5, "C", "");   // 5:CC>C
    }

    /**
     * This test normalizes two variants and checks that all their attributes are copied. One of them has the default
     * flag values and the other doesn't.
     */
    @Test
    public void variantsAreCopiedCompletely() {
        DbsnpSubmittedVariantEntity variant = new DbsnpSubmittedVariantEntity(SS_ID, HASH, ASSEMBLY, TAXONOMY, PROJECT,
                                                                              "22", 3, "", "G", RS_ID,
                                                                              DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                              DEFAULT_ASSEMBLY_MATCH,
                                                                              DEFAULT_ALLELES_MATCH,
                                                                              DEFAULT_VALIDATED, 2);
        long otherSsId = SS_ID + 1;
        String otherHash = "other";
        long otherRsId = RS_ID + 1;
        String otherProject = "otherProject";
        DbsnpSubmittedVariantEntity variant2 = new DbsnpSubmittedVariantEntity(otherSsId, otherHash, ASSEMBLY, TAXONOMY,
                                                                               otherProject, "22", 6, "C", "", otherRsId,
                                                                               !DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                                               !DEFAULT_ASSEMBLY_MATCH,
                                                                               !DEFAULT_ALLELES_MATCH,
                                                                               !DEFAULT_VALIDATED, 3);

        List<DbsnpSubmittedVariantEntity> normalizedVariants = renormalizer.process(Arrays.asList(variant, variant2));

        // The version is not compared in the equals method, so we have to compare it explicitly
        assertEquals(new DbsnpSubmittedVariantEntity(SS_ID, HASH, ASSEMBLY, TAXONOMY, PROJECT, "22", 2, "", "G", RS_ID,
                                                     DEFAULT_SUPPORTED_BY_EVIDENCE, DEFAULT_ASSEMBLY_MATCH,
                                                     DEFAULT_ALLELES_MATCH, DEFAULT_VALIDATED, 2),
                     normalizedVariants.get(0));
        assertEquals(2, normalizedVariants.get(0).getVersion());

        assertEquals(new DbsnpSubmittedVariantEntity(otherSsId, otherHash, ASSEMBLY, TAXONOMY, otherProject, "22",
                                                     5, "C", "", otherRsId, !DEFAULT_SUPPORTED_BY_EVIDENCE,
                                                     !DEFAULT_ASSEMBLY_MATCH, !DEFAULT_ALLELES_MATCH,
                                                     !DEFAULT_VALIDATED, 3),
                     normalizedVariants.get(1));
        assertEquals(3, normalizedVariants.get(1).getVersion());
    }
}
