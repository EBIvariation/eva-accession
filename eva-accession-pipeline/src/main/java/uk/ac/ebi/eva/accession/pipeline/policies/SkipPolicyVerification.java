package uk.ac.ebi.eva.accession.pipeline.policies;

import org.springframework.batch.core.step.skip.SkipLimitExceededException;
import org.springframework.batch.core.step.skip.SkipPolicy;
import org.springframework.batch.item.file.FlatFileParseException;

import uk.ac.ebi.eva.commons.core.models.factories.exception.IncompleteInformationException;
import uk.ac.ebi.eva.commons.core.models.factories.exception.NonVariantException;

public class SkipPolicyVerification implements SkipPolicy {

    @Override
    public boolean shouldSkip(Throwable exception, int skipCount) throws SkipLimitExceededException {
        if (exception instanceof FlatFileParseException
                && (exception.getCause() instanceof NonVariantException
                || exception.getCause() instanceof IncompleteInformationException)) {
            return true;
        }
        return false;
    }
}
