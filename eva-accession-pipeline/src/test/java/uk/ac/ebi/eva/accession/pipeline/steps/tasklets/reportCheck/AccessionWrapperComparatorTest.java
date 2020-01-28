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
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class AccessionWrapperComparatorTest {

    private static final String CONTIG_A = "A";

    private static final String CONTIG_B = "B";

    private static final String CONTIG_2 = "2";

    private static final String CONTIG_11 = "11";

    private static final String CONTIG_PREFIX_2 = "prefix2";

    private static final String CONTIG_PREFIX_11 = "prefix11";

    private static final String REPLACED_CONTIG = "contig_to_replace";

    private static final String REPLACEMENT_CONTIG = "replacement_contig";

    private AccessionWrapperComparator accessionWrapperComparator;

    @Rule
    public ExpectedException thrown = ExpectedException.none();

    private Map<String, String> contigMapping;

    @Before
    public void setUp() throws Exception {
        List<Variant> variants = Arrays.asList(buildMockVariant(CONTIG_A),
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
                                      new SubmittedVariant("", 0, "", contig, position, "", "", null, null, null, null,
                                                           null, null));
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
        List<Variant> variants = Arrays.asList(buildMockVariant(CONTIG_B),
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

    private Variant buildMockVariant(String contig) {
        return new Variant(contig, 0,  0, "", "");
    }

    private SubmittedVariant buildMockSubmittedVariant(String contig) {
        return new SubmittedVariant("", 0, "", contig, 0, "", "", null, null, null, null, null, null);
    }

    @Test
    public void checkUnexpectedContigRaisesException() {
        List<Variant> variants = Arrays.asList(buildMockVariant(CONTIG_B),
                                               buildMockVariant(CONTIG_A));
        AccessionWrapperComparator comparator = new AccessionWrapperComparator(variants);

        thrown.expect(IllegalStateException.class);
        comparator.compare(buildMockAccessionWrapper(CONTIG_11, 100),
                           buildMockAccessionWrapper(CONTIG_2, 100));
    }
}
