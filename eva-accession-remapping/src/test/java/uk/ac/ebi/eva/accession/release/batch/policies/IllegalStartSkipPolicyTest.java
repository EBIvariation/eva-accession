/*
 * Copyright 2019 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.accession.release.batch.policies;

import org.junit.Before;
import org.junit.Test;

import uk.ac.ebi.eva.accession.core.exceptions.PositionOutsideOfContigException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class IllegalStartSkipPolicyTest {

    private IllegalStartSkipPolicy illegalStartSkipPolicy;

    @Before
    public void setUp() throws Exception {
        illegalStartSkipPolicy = new IllegalStartSkipPolicy();
    }

    @Test
    public void shouldSkipException() {
        Throwable illegalStartPositionException = new PositionOutsideOfContigException("exception");
        assertTrue(illegalStartSkipPolicy.shouldSkip(illegalStartPositionException, 0));
    }

    @Test
    public void shouldNotSkipException() {
        Throwable illegalArgumentException = new IllegalArgumentException("exception");
        assertFalse(illegalStartSkipPolicy.shouldSkip(illegalArgumentException, 0));
    }

}