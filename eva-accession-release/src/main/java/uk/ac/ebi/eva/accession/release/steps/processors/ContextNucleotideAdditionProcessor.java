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

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import static java.lang.Math.max;

public class ContextNucleotideAdditionProcessor implements ItemProcessor<Variant, IVariant> {

    private static Logger logger = LoggerFactory.getLogger(ContextNucleotideAdditionProcessor.class);

    private FastaSynonymSequenceReader fastaSequenceReader;

    public ContextNucleotideAdditionProcessor(FastaSynonymSequenceReader fastaReader) {
        this.fastaSequenceReader = fastaReader;
    }

    @Override
    public IVariant process(Variant variant) throws Exception {
        String contig = variant.getChromosome();

        if (fastaSequenceReader.doesContigExist(contig)) {
            return getVariantWithContextNucleotide(variant);
        } else {
            throw new IllegalArgumentException("Contig '" + contig + "' does not appear in the FASTA file ");
        }
    }

    private IVariant getVariantWithContextNucleotide(Variant variant) {
        long oldStart = variant.getStart();
        String contig = variant.getChromosome();
        String oldReference = variant.getReference();
        String oldAlternate = variant.getAlternate();

        if (variant.getType().equals(VariantType.SEQUENCE_ALTERATION)) {
            return renormalizeNamedVariant(variant, oldStart, contig, oldReference, oldAlternate);
        } else if (oldReference.isEmpty() || oldAlternate.isEmpty()) {
            return renormalizeIndel(variant, oldStart, contig, oldReference, oldAlternate);
        }
        return variant;
    }

    /**
     * Transform a named variant into a structural variant with symbolic alleles.
     *
     * Examples of structural variants taken from
     * <a href='https://samtools.github.io/hts-specs/VCFv4.3.pdf'>VCFv.3 spec</a> (section 5.3):
     * <pre>
     * {@code
     * 2       321682 .    T    <DEL>        6    PASS   SVTYPE=DEL;END=321887;SVLEN=-205
     * 2     14477084 .    C    <DEL:ME:ALU> 12   PASS   SVTYPE=DEL;END=14477381;SVLEN=-297
     * 3      9425916 .    C    <INS:ME:L1>  23   PASS   SVTYPE=INS;END=9425916;SVLEN=6027
     * }
     * </pre>
     */
    private Variant renormalizeNamedVariant(Variant variant, long oldStart, String contig, String oldReference,
                                         String oldAlternate) {
        String newReference;
        String newAlternate;
        ImmutableTriple<Long, String, String> startAndReferenceAndAlternate =
                fastaSequenceReader.getContextNucleotideAndNewStart(
                        contig, oldStart, "", "");
        long newStart = startAndReferenceAndAlternate.getLeft();
        long newEnd = newStart; // TODO jmmut: can we find out the real end? named variants are not easy to parse

        if (oldReference.isEmpty() && isNamedAllele(oldAlternate)) {
            newReference = startAndReferenceAndAlternate.getMiddle();

            // note that we are not putting the context base in the alternate. (Read this method's doc)
            newAlternate = oldAlternate;
        } else if (isNamedAllele(oldReference) && oldAlternate.isEmpty()) {
            newReference = startAndReferenceAndAlternate.getMiddle();

            // note that we are putting the named reference as the alternate allele. (Read this method's doc)
            newAlternate = oldReference;
        } else {
            logger.warn(
                    "Filtering out variant because a context nucleotide could not be added: given the reference and "
                    + "alternate alleles, one should be a named allele (surrounded by parenthesis) and the other "
                    + "should be empty. This variant will be filtered out: " + variant);
            return null;
        }
        variant.renormalize(newStart, newEnd, newReference, newAlternate);
        return variant;
    }

    private boolean isNamedAllele(String allele) {
        return allele.startsWith("(") && allele.endsWith(")");
    }

    private Variant renormalizeIndel(Variant variant, long oldStart, String contig, String oldReference,
                                  String oldAlternate) {
        String newReference;
        String newAlternate;
        ImmutableTriple<Long, String, String> contextNucleotideInfo =
                fastaSequenceReader.getContextNucleotideAndNewStart(
                        contig, oldStart, oldReference, oldAlternate);

        long newStart = contextNucleotideInfo.getLeft();
        newReference = contextNucleotideInfo.getMiddle();
        newAlternate = contextNucleotideInfo.getRight();
        long newEnd = newStart + max(newReference.length(), newAlternate.length()) - 1;
        variant.renormalize(newStart, newEnd, newReference, newAlternate);
        return variant;
    }
}
