/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.pipeline.policies;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.pipeline.configuration.InvalidVariantSkipPolicyConfiguration;
import uk.ac.ebi.eva.commons.core.models.factories.exception.IncompleteInformationException;
import uk.ac.ebi.eva.commons.core.models.factories.exception.NonVariantException;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@RunWith(SpringRunner.class)
@Import({InvalidVariantSkipPolicyConfiguration.class})
public class InvalidVariantSkipPolicyTest {

    @Mock
    Variant variant;

    private InvalidVariantSkipPolicy invalidVariantSkipPolicy;

    @Before
    public void setUp() throws Exception {
        invalidVariantSkipPolicy = new InvalidVariantSkipPolicy();
    }

    @Test
    public void skipFlatFileParseExceptionCausedByNonVariantException() {
        Throwable nonVariantException = new NonVariantException("NonVariantException");
        Throwable flatFileParseException = new FlatFileParseException("FlatFileParseException", nonVariantException,
                                                                      "input", 100);
        assertTrue(invalidVariantSkipPolicy.shouldSkip(flatFileParseException, 10));
    }

    @Test
    public void skipFlatFileParseExceptionCausedByIncompleteInformationException() {
        Throwable incompleteInformationException = new IncompleteInformationException(variant);
        Throwable flatFileParseException = new FlatFileParseException("FlatFileParseException",
                                                                      incompleteInformationException, "input", 100);
        assertTrue(invalidVariantSkipPolicy.shouldSkip(flatFileParseException, 10));
    }

    @Test
    public void failOnFlatFileParseExceptionCausedByException() {
        Throwable exception = new Exception("Exception");
        Throwable flatFileParseException = new FlatFileParseException("FlatFileParseException", exception, "input",
                                                                      100);
        assertFalse(invalidVariantSkipPolicy.shouldSkip(flatFileParseException, 10));
    }

    @Test
    public void failOnException() {
        Throwable exception = new Exception("Exception");
        assertFalse(invalidVariantSkipPolicy.shouldSkip(exception, 10));
    }
}
