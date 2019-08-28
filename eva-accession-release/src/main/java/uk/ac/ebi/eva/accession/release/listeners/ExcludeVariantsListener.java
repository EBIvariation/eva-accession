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

package uk.ac.ebi.eva.accession.release.listeners;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.ExitStatus;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.listener.StepListenerSupport;

import uk.ac.ebi.eva.commons.core.models.AbstractVariant;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.Set;
import java.util.stream.Collectors;

public class ExcludeVariantsListener extends StepListenerSupport<IVariant, IVariant> {

    private static final Logger logger = LoggerFactory.getLogger(ExcludeVariantsListener.class);

    @Override
    public ExitStatus afterStep(StepExecution stepExecution) {
        logger.info("Processors filtered out {} variants", stepExecution.getFilterCount());
        logger.warn("Processors skipped {} variants because the start position is greater than the chromosome end",
                    stepExecution.getSkipCount());
        return stepExecution.getExitStatus();
    }
}
