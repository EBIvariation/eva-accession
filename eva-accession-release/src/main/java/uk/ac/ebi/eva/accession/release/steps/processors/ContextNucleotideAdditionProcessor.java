/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.release.steps.processors;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.dbsnp.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static java.lang.Math.max;

public class ContextNucleotideAdditionProcessor implements ItemProcessor<IVariant, IVariant> {

    private FastaSynonymSequenceReader fastaSequenceReader;

    public ContextNucleotideAdditionProcessor(FastaSynonymSequenceReader fastaReader) {
        this.fastaSequenceReader = fastaReader;
    }

    @Override
    public IVariant process(final IVariant variant) throws Exception {
        String newReference;
        String newAlternate;

        long oldStart = variant.getStart();
        String oldReference = variant.getReference();
        String oldAlternate = variant.getAlternate();
        if (oldReference.isEmpty() || oldAlternate.isEmpty()) {
            ImmutablePair<String, Long> contextNucleotideInfo =
                    fastaSequenceReader.getContextNucleotide(variant.getChromosome(), oldStart, oldReference,
                                                             oldAlternate);
            String contextBase = contextNucleotideInfo.getLeft();
            long newStart = contextNucleotideInfo.getRight();
            if (oldStart == 1) {
                newReference = oldReference + contextBase;
                newAlternate = oldAlternate + contextBase ;
            }
            else {
                newReference = contextBase + oldReference;
                newAlternate = contextBase + oldAlternate;
            }
            long newEnd = newStart + max(newReference.length(), newAlternate.length()) - 1;
            if (contextBase.isEmpty()) {
                throw new IllegalStateException("fastaSequenceReader should have returned a non-empty sequence");
            } else {
                return new Variant(variant.getChromosome(), newStart, newEnd, newReference, newAlternate);
            }
        }
        else {
            return variant;
        }
    }
}
