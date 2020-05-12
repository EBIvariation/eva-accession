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

package uk.ac.ebi.eva.accession.release.configuration.batch.processors;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.release.parameters.InputParameters;
import uk.ac.ebi.eva.accession.release.batch.processors.ContextNucleotideAdditionProcessor;
import uk.ac.ebi.eva.accession.release.batch.processors.ExcludeInvalidVariantsProcessor;
import uk.ac.ebi.eva.accession.release.batch.processors.NamedVariantProcessor;
import uk.ac.ebi.eva.accession.release.batch.processors.VariantToVariantContextProcessor;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

import static uk.ac.ebi.eva.accession.release.configuration.BeanNames.RELEASE_PROCESSOR;

@Configuration
public class ReleaseProcessorConfiguration {

    @Bean(RELEASE_PROCESSOR)
    public ItemProcessor<Variant, VariantContext> releaseProcessor(FastaSynonymSequenceReader fastaReader,
                                                                   ContigMapping contigMapping) {
        CompositeItemProcessor<Variant, VariantContext> compositeItemProcessor = new CompositeItemProcessor<>();
        compositeItemProcessor.setDelegates(Arrays.asList(new NamedVariantProcessor(),
                                                          new ExcludeInvalidVariantsProcessor(),
                                                          new ContextNucleotideAdditionProcessor(fastaReader),
                                                          new ExcludeInvalidVariantsProcessor(),
                                                          new VariantToVariantContextProcessor(contigMapping)));
        return compositeItemProcessor;
    }

    @Bean
    FastaSynonymSequenceReader fastaSynonymSequenceReader(ContigMapping contigMapping, InputParameters parameters)
            throws IOException {
        Path referenceFastaFile = Paths.get(parameters.getFasta());
        return new FastaSynonymSequenceReader(contigMapping, referenceFastaFile);
    }

    @Bean
    ContigMapping contigMapping(InputParameters parameters) throws Exception {
        return new ContigMapping(parameters.getAssemblyReportUrl());
    }

}
