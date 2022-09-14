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

package uk.ac.ebi.eva.remapping.source.configuration.batch.processors;

import htsjdk.variant.variantcontext.VariantContext;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import uk.ac.ebi.eva.accession.core.batch.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.batch.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.core.contig.ContigMapping;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.remapping.source.batch.processors.ContextNucleotideAdditionProcessor;
import uk.ac.ebi.eva.remapping.source.batch.processors.ExcludeAllelesMismatchVariantsProcessor;
import uk.ac.ebi.eva.remapping.source.batch.processors.ExcludeInvalidVariantsProcessor;
import uk.ac.ebi.eva.remapping.source.batch.processors.ExcludeMultimapVariantsProcessor;
import uk.ac.ebi.eva.remapping.source.batch.processors.SubmittedVariantToVariantContextProcessor;
import uk.ac.ebi.eva.remapping.source.configuration.BeanNames;
import uk.ac.ebi.eva.remapping.source.parameters.InputParameters;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

@Configuration
public class SubmittedVariantsProcessorConfiguration {

    @Bean(BeanNames.SUBMITTED_VARIANT_PROCESSOR)
    public ItemProcessor<SubmittedVariantEntity, VariantContext> submittedVariantProcessor(FastaSequenceReader fastaReader) {
        CompositeItemProcessor<SubmittedVariantEntity, VariantContext> compositeItemProcessor =
                new CompositeItemProcessor<>();

        compositeItemProcessor.setDelegates(Arrays.asList(
                new ExcludeMultimapVariantsProcessor(),
                new ExcludeAllelesMismatchVariantsProcessor(),
                new ExcludeInvalidVariantsProcessor(),
                new ContextNucleotideAdditionProcessor(fastaReader),
                new ExcludeInvalidVariantsProcessor(),  // exclude again in case a IUPAC code is added as context base
                new SubmittedVariantToVariantContextProcessor()));

        return compositeItemProcessor;
    }

    @Bean
    FastaSequenceReader fastaSequenceReader(InputParameters parameters)
            throws Exception {
        if (parameters.getAssemblyReportUrl().isEmpty()) {
            return new FastaSequenceReader(Paths.get(parameters.getFasta()));
        } else {
            Path referenceFastaFile = Paths.get(parameters.getFasta());
            ContigMapping contigMapping = new ContigMapping(parameters.getAssemblyReportUrl());
            return new FastaSynonymSequenceReader(contigMapping, referenceFastaFile);
        }
    }
}
