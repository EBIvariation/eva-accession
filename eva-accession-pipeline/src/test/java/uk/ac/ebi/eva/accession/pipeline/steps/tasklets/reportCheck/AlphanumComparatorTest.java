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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class AlphanumComparatorTest {

    @Test
    public void stringsAndNumbers() {
        List<String> expectedOrdering = Arrays.asList(
                "1",
                "2",
                "10",
                "EctoMorph6",
                "EctoMorph7",
                "EctoMorph62",
                "dazzle1",
                "dazzle2",
                "dazzle2.7",
                "dazzle2.10",
                "dazzle10",
                "fizzle"
        );
        List<String> actualOrdering = new ArrayList<>(expectedOrdering);
        Collections.shuffle(actualOrdering);
        assertNotEquals(expectedOrdering, actualOrdering);
        actualOrdering.sort(new AlphanumComparator());
        assertEquals(expectedOrdering, actualOrdering);
    }

    @Test
    public void strings() {
        assertEquals(-1, new AlphanumComparator().compare("chrA", "chrB"));
        assertEquals(1, new AlphanumComparator().compare("chrB", "chrA"));
    }

    @Test
    public void numbers() {
        assertEquals(0, new AlphanumComparator().compare("1", "1"));

        assertEquals(-1, new AlphanumComparator().compare("1", "2"));
        assertEquals(-1, new AlphanumComparator().compare("1", "11"));
        assertEquals(-1, new AlphanumComparator().compare("2", "11"));

        assertEquals(1, new AlphanumComparator().compare("2", "1"));
        assertEquals(1, new AlphanumComparator().compare("11", "1"));
        assertEquals(1, new AlphanumComparator().compare("11", "2"));
    }

    @Test
    public void numbersWithStringPrefix() {
        assertEquals(0, new AlphanumComparator().compare("chr1", "chr1"));

        assertEquals(-1, new AlphanumComparator().compare("chr1", "chr2"));
        assertEquals(-1, new AlphanumComparator().compare("chr1", "chr11"));
        assertEquals(-1, new AlphanumComparator().compare("chr2", "chr11"));

        assertEquals(1, new AlphanumComparator().compare("chr2", "chr1"));
        assertEquals(1, new AlphanumComparator().compare("chr11", "chr1"));
        assertEquals(1, new AlphanumComparator().compare("chr11", "chr2"));
    }
}
