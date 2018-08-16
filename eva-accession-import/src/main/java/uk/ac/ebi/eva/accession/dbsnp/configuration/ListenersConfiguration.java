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
package uk.ac.ebi.eva.accession.dbsnp.configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportCounts;
import uk.ac.ebi.eva.accession.dbsnp.listeners.ImportDbsnpVariantsStepProgressListener;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.parameters.InputParameters;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.ASSEMBLY_CHECK_STEP_LISTENER;
import static uk.ac.ebi.eva.accession.dbsnp.configuration.BeanNames.IMPORT_DBSNP_VARIANTS_PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {

    private static final Logger logger = LoggerFactory.getLogger(ListenersConfiguration.class);

    @Bean(ASSEMBLY_CHECK_STEP_LISTENER)
    StepListenerSupport assemblyCheckStepListener(FastaSequenceReader fastaSequenceReader) {
        return new StepListenerSupport() {
            @Override
            public ExitStatus afterStep(StepExecution stepExecution) {
                try {
                    logger.debug("Closing FASTA file reader used for assembly check");
                    fastaSequenceReader.close();
                } catch (Exception e) {
                    logger.warn("Error closing FASTA file reader used for assembly check: {}", e.getMessage());
                }
                return stepExecution.getExitStatus();
            }
        };
    }

    @Bean
    @StepScope
    public ImportCounts importCounts() {
        return new ImportCounts();
    }

    @Bean(IMPORT_DBSNP_VARIANTS_PROGRESS_LISTENER)
    public StepListenerSupport<SubSnpNoHgvs, DbsnpVariantsWrapper> importDbsnpVariantsProgressListener(
            InputParameters parameters, ImportCounts importCounts) {
        return new ImportDbsnpVariantsStepProgressListener(parameters.getChunkSize(), importCounts);
    }

}
