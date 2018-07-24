package uk.ac.ebi.eva.accession.core.persistence;

import uk.ac.ebi.ampt2d.commons.accession.core.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.SaveResponse;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionIsNotPendingException;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicAccessionGenerator;
import uk.ac.ebi.ampt2d.commons.accession.generators.monotonic.MonotonicRangePriorityQueue;
import uk.ac.ebi.ampt2d.commons.accession.persistence.jpa.monotonic.service.ContiguousIdBlockService;

import java.util.List;
import java.util.Map;

public class DbsnpMonotonicAccessionGenerator<T> extends MonotonicAccessionGenerator {

    public DbsnpMonotonicAccessionGenerator(long blockSize, String categoryId, String applicationInstanceId,
                                            ContiguousIdBlockService contiguousIdBlockService) {
        super(blockSize, categoryId, applicationInstanceId, contiguousIdBlockService);
    }

    @Override
    public synchronized void recoverState(long[] committedElements) throws AccessionIsNotPendingException {
        throw new UnsupportedOperationException("New accessions should not be generated");
    }

    @Override
    public synchronized long[] generateAccessions(
            int numAccessionsToGenerate) throws AccessionCouldNotBeGeneratedException {
        throw new UnsupportedOperationException("New accessions should not be generated");
    }

    @Override
    public synchronized void commit(long... accessions) throws AccessionIsNotPendingException {
        throw new UnsupportedOperationException("New accessions should not be generated");
    }

    @Override
    public synchronized void release(long... accessions) throws AccessionIsNotPendingException {
        throw new UnsupportedOperationException("New accessions should not be generated");
    }

    @Override
    public synchronized MonotonicRangePriorityQueue getAvailableRanges() {
        throw new UnsupportedOperationException("New accessions should not be generated");
    }

    @Override
    public synchronized void postSave(SaveResponse response) {
        throw new UnsupportedOperationException("New accessions should not be generated");
    }

    @Override
    public List<AccessionWrapper> generateAccessions(Map messages) throws AccessionCouldNotBeGeneratedException {
        throw new UnsupportedOperationException("New accessions should not be generated");
    }
}
