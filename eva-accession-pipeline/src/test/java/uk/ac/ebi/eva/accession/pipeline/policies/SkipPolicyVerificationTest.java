package uk.ac.ebi.eva.accession.pipeline.policies;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.springframework.batch.item.file.FlatFileParseException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.junit4.SpringRunner;

import uk.ac.ebi.eva.accession.pipeline.configuration.SkipPolicyVerificationConfiguration;
import uk.ac.ebi.eva.commons.core.models.factories.exception.IncompleteInformationException;
import uk.ac.ebi.eva.commons.core.models.factories.exception.NonVariantException;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static org.junit.Assert.assertEquals;

@RunWith(SpringRunner.class)
@Import({SkipPolicyVerificationConfiguration.class})
public class SkipPolicyVerificationTest {

    @Mock
    Variant variant;

    @Autowired
    private SkipPolicyVerification skipPolicyVerification;

    @Test
    public void skipFlatFileParseExceptionCausedByNonVariantException() {
        Throwable nonVariantException = new NonVariantException("NonVariantException");
        Throwable flatFileParseException = new FlatFileParseException("FlatFileParseException", nonVariantException,
                                                                      "input", 100);
        assertEquals(skipPolicyVerification.shouldSkip(flatFileParseException, 10), true);
    }

    @Test
    public void skipFlatFileParseExceptionCausedByIncompleteInformationException() {
        Throwable incompleteInformationException = new IncompleteInformationException(variant);
        Throwable flatFileParseException = new FlatFileParseException("FlatFileParseException",
                                                                      incompleteInformationException, "input", 100);
        assertEquals(skipPolicyVerification.shouldSkip(flatFileParseException, 10), true);
    }

    @Test
    public void failOnFlatFileParseExceptionCausedByException() {
        Throwable exception = new Exception("Exception");
        Throwable flatFileParseException = new FlatFileParseException("FlatFileParseException", exception, "input",
                                                                      100);
        assertEquals(skipPolicyVerification.shouldSkip(flatFileParseException, 10), false);
    }

    @Test
    public void failOnException() {
        Throwable exception = new Exception("Exception");
        assertEquals(skipPolicyVerification.shouldSkip(exception, 10), false);
    }
}
