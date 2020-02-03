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

package uk.ac.ebi.eva.accession.release.configuration.batch.listeners;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.core.listener.StepListenerSupport;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.release.batch.listeners.ExcludeVariantsListener;
import uk.ac.ebi.eva.accession.core.batch.listeners.GenericProgressListener;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.EXCLUDE_VARIANTS_LISTENER;
import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.PROGRESS_LISTENER;

@Configuration
public class ListenersConfiguration {

    @Bean(EXCLUDE_VARIANTS_LISTENER)
    public StepListenerSupport excludeVariantsListener() {
        return new ExcludeVariantsListener();
    }

    @Bean(PROGRESS_LISTENER)
    public StepListenerSupport<Variant, VariantContext> releaseDbsnpVariantsProgressListener(
            InputParameters parameters) {
        return new GenericProgressListener<>(parameters.getChunkSize());
    }
}
