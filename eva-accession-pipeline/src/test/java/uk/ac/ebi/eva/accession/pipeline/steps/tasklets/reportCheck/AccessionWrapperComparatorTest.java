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

import org.junit.Test;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;

import static org.junit.Assert.assertEquals;

public class AccessionWrapperComparatorTest {

    @Test
    public void checkSortedContig() {
        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator();
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper("A", 200),
                                                             buildMockAccessionWrapper("B", 100));
        assertEquals(-1, comparation);
    }

    private AccessionWrapper<ISubmittedVariant, String, Long> buildMockAccessionWrapper(String contig, int position) {
        return new AccessionWrapper<>(null,
                                      null,
                                      new SubmittedVariant("", 0, "", contig, position, "", "", false));
    }

    @Test
    public void checkUnsortedContig() {
        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator();
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper("B", 100),
                                                             buildMockAccessionWrapper("A", 200));
        assertEquals(1, comparation);
    }

    @Test
    public void checkEqualContigSortedPosition() {
        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator();
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper("A", 100),
                                                             buildMockAccessionWrapper("A", 200));
        assertEquals(-1, comparation);
    }

    @Test
    public void checkEqualContigUnsortedPosition() {
        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator();
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper("A", 200),
                                                             buildMockAccessionWrapper("A", 100));
        assertEquals(1, comparation);
    }

    @Test
    public void checkEqualContigEqualPosition() {
        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator();
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper("A", 100),
                                                             buildMockAccessionWrapper("A", 100));
        assertEquals(0, comparation);
    }

    @Test
    public void checkNumericalContig() {
        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator();
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper("2", 100),
                                                             buildMockAccessionWrapper("11", 100));
        assertEquals(-1, comparation);
    }
    @Test
    public void checkNumericalContigWithPrefix() {
        AccessionWrapperComparator accessionWrapperComparator = new AccessionWrapperComparator();
        int comparation = accessionWrapperComparator.compare(buildMockAccessionWrapper("prefix2", 100),
                                                             buildMockAccessionWrapper("prefix11", 100));
        assertEquals(-1, comparation);
    }


}
