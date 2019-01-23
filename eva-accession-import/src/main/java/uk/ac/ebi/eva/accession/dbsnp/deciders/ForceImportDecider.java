package uk.ac.ebi.eva.accession.dbsnp.deciders;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.job.flow.FlowExecutionStatus;
import org.springframework.batch.core.job.flow.JobExecutionDecider;
import org.springframework.beans.factory.annotation.Autowired;

import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportDbsnpVariantsStepProgressListener;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;

public class ForceImportDecider implements JobExecutionDecider {

    private static final Logger logger = LoggerFactory.getLogger(ImportDbsnpVariantsStepProgressListener.class);

    @Autowired
    private InputParameters inputParameters;

    @Override
    public FlowExecutionStatus decide(JobExecution jobExecution, StepExecution stepExecution) {
        logger.info("Continue import when a contig not found in assembly report: {}", inputParameters.getForceImport());
        String forceImport = inputParameters.getForceImport().toUpperCase();
        return new FlowExecutionStatus(forceImport);
    }
}
