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
package uk.ac.ebi.eva.accession.release.configuration.batch.steps;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.StepExecutionListener;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.step.tasklet.TaskletStep;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemStreamWriter;
import org.springframework.batch.repeat.policy.SimpleCompletionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import uk.ac.ebi.eva.accession.core.batch.io.AccessionedVcfLineMapper;
import uk.ac.ebi.eva.accession.core.batch.policies.IllegalStartSkipPolicy;
import uk.ac.ebi.eva.accession.release.batch.io.ReleaseRecordWriter;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.AccessionedVariantMongoReaderConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.io.VariantContextWriterConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.listeners.ListenersConfiguration;
import uk.ac.ebi.eva.accession.release.configuration.batch.processors.ReleaseProcessorConfiguration;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.commons.batch.io.UnwindingItemStreamReader;
import uk.ac.ebi.eva.commons.batch.io.VcfReader;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.File;
import java.io.IOException;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.*;

@Configuration
@Import({AccessionedVariantMongoReaderConfiguration.class,
         ReleaseProcessorConfiguration.class,
         VariantContextWriterConfiguration.class,
         ListenersConfiguration.class})
public class CreateIncrementalReleaseStepConfiguration {

    @Autowired
    private InputParameters inputParameters;

    @Autowired
    @Qualifier(PROGRESS_LISTENER)
    private StepExecutionListener progressListener;

    @Autowired
    @Qualifier(EXCLUDE_VARIANTS_LISTENER)
    private StepExecutionListener excludeVariantsListener;

    @Autowired
    private IllegalStartSkipPolicy illegalStartSkipPolicy;

    public ItemStreamReader<Variant> accessionedVcfReader() throws IOException {
        VcfReader vcfReader = new VcfReader(new AccessionedVcfLineMapper(),
                new File(inputParameters.getAccessionedVcf()));
        return new UnwindingItemStreamReader<>(vcfReader);
    }

    @Bean(CREATE_INCREMENTAL_ACCESSION_RELEASE_STEP)
    public Step createIncrementalAccessionReleaseStep(
            StepBuilderFactory stepBuilderFactory,
            SimpleCompletionPolicy chunkSizeCompletionPolicy,
            @Qualifier(RELEASE_PROCESSOR) ItemProcessor<Variant, VariantContext> releaseProcessor,
            @Qualifier(INCREMENTAL_RELEASE_WRITER) ReleaseRecordWriter releaseRecordWriter)
            throws IOException {
        TaskletStep step = stepBuilderFactory.get(RELEASE_DBSNP_MAPPED_ACTIVE_VARIANTS_STEP)
                .<Variant, VariantContext>chunk(chunkSizeCompletionPolicy)
                .reader(accessionedVcfReader())
                .processor(releaseProcessor)
                .writer(releaseRecordWriter)
                .faultTolerant()
                .skipPolicy(illegalStartSkipPolicy)
                .listener(excludeVariantsListener)
                .listener(progressListener)
                .build();
        return step;
    }
}
