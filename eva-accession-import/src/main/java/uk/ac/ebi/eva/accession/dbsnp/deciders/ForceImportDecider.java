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
        String forceImport = inputParameters.getForceImport().toUpperCase();
        logger.info("Continue importing if a contig is not found in assembly report: {}", forceImport);
        return new FlowExecutionStatus(forceImport);
    }
}
