package uk.ac.ebi.eva.accession.release.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepListenerSupport;

import uk.ac.ebi.eva.commons.core.models.IVariant;

public class ExcludeVariantsListener extends StepListenerSupport<IVariant, IVariant> {

    private static final Logger logger = LoggerFactory.getLogger(ExcludeVariantsListener.class);

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Processors filtered out {} variants", stepExecution.getProcessSkipCount());
        return stepExecution.getExitStatus();
    }
}
