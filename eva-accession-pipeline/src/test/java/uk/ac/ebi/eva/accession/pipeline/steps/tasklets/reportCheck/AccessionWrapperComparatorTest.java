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
package uk.ac.ebi.eva.accession.pipeline.steps.tasklets.reportCheck;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class AccessionWrapperComparatorTest {

    private static final String CONTIG_A = "A";

    private static final String CONTIG_B = "B";

    private static final String CONTIG_2 = "2";

    private static final String CONTIG_11 = "11";

    private static final String CONTIG_PREFIX_2 = "prefix2";

    private static final String CONTIG_PREFIX_11 = "prefix11";

    private AccessionWrapperComparator accessionWrapperComparator;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    @Before
    public void setUp() throws Exception {
        List<SubmittedVariant> variants = Arrays.asList(buildMockVariant(CONTIG_A),
                                                        buildMockVariant(CONTIG_B),
                                                        buildMockVariant(CONTIG_2),
                                                        buildMockVariant(CONTIG_11),
                                                        buildMockVariant(CONTIG_PREFIX_2),
                                                        buildMockVariant(CONTIG_PREFIX_11)
        );
        accessionWrapperComparator = new AccessionWrapperComparator(variants);
    }

    @Test
    public void checkSortedContig() {
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper(CONTIG_A, 200),
                                                             buildMockAccessionWrapper(CONTIG_B, 100));
        assertEquals(-1, comparation);
    }

    private AccessionWrapper<ISubmittedVariant, String, Long> buildMockAccessionWrapper(String contig, int position) {
        return new AccessionWrapper<>(null,
                                      null,
                                      new SubmittedVariant("", 0, "", contig, position, "", "", false));
    }

    @Test
    public void checkUnsortedContig() {
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper(CONTIG_B, 100),
                                                             buildMockAccessionWrapper(CONTIG_A, 200));
        assertEquals(1, comparation);
    }

    @Test
    public void checkEqualContigSortedPosition() {
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper(CONTIG_A, 100),
                                                             buildMockAccessionWrapper(CONTIG_A, 200));
        assertEquals(-1, comparation);
    }

    @Test
    public void checkEqualContigUnsortedPosition() {
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper(CONTIG_A, 200),
                                                             buildMockAccessionWrapper(CONTIG_A, 100));
        assertEquals(1, comparation);
    }

    @Test
    public void checkEqualContigEqualPosition() {
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper(CONTIG_A, 100),
                                                             buildMockAccessionWrapper(CONTIG_A, 100));
        assertEquals(0, comparation);
    }

    @Test
    public void checkNumericalContig() {
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper(CONTIG_2, 100),
                                                             buildMockAccessionWrapper(CONTIG_11, 100));
        assertEquals(-1, comparation);
    }

    @Test
    public void checkNumericalContigWithPrefix() {
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper(CONTIG_PREFIX_2, 100),
                                                             buildMockAccessionWrapper(CONTIG_PREFIX_11, 100));
        assertEquals(-1, comparation);
    }

    @Test
    public void checkDifferentInputOrder() {
        List<SubmittedVariant> variants = Arrays.asList(buildMockVariant(CONTIG_B),
                                                        buildMockVariant(CONTIG_A),
                                                        buildMockVariant(CONTIG_A),
                                                        buildMockVariant(CONTIG_A),
                                                        buildMockVariant(CONTIG_11),
                                                        buildMockVariant(CONTIG_11),
                                                        buildMockVariant(CONTIG_2),
                                                        buildMockVariant(CONTIG_2),
                                                        buildMockVariant(CONTIG_2));
        AccessionWrapperComparator comparator = new AccessionWrapperComparator(variants);

        assertEquals(-1, comparator.compare(buildMockAccessionWrapper(CONTIG_11, 100),
                                            buildMockAccessionWrapper(CONTIG_2, 100)));
        assertEquals(+1, comparator.compare(buildMockAccessionWrapper(CONTIG_2, 100),
                                            buildMockAccessionWrapper(CONTIG_11, 100)));

        assertEquals(-1, comparator.compare(buildMockAccessionWrapper(CONTIG_B, 100),
                                            buildMockAccessionWrapper(CONTIG_11, 100)));
        assertEquals(+1, comparator.compare(buildMockAccessionWrapper(CONTIG_2, 100),
                                            buildMockAccessionWrapper(CONTIG_A, 100)));
    }

    private SubmittedVariant buildMockVariant(String contig) {
        return new SubmittedVariant("", 0, "", contig, 0, "", "", false);
    }

    @Test
    public void checkUnexpectedContigRaisesException() {
        List<SubmittedVariant> variants = Arrays.asList(buildMockVariant(CONTIG_B),
                                                        buildMockVariant(CONTIG_A));
        AccessionWrapperComparator comparator = new AccessionWrapperComparator(variants);

        thrown.expect(IllegalStateException.class);
        comparator.compare(buildMockAccessionWrapper(CONTIG_11, 100),
                           buildMockAccessionWrapper(CONTIG_2, 100));
    }
}
