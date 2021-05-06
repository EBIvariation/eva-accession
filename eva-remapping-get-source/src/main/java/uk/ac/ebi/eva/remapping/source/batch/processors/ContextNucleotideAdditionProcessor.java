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
package uk.ac.ebi.eva.remapping.source.batch.processors;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.batch.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.exceptions.PositionOutsideOfContigException;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Arrays;
import java.util.regex.Pattern;

import static java.lang.Math.max;

public class ContextNucleotideAdditionProcessor
        implements ItemProcessor<SubmittedVariantEntity, SubmittedVariantEntity> {

    private static Logger logger = LoggerFactory.getLogger(ContextNucleotideAdditionProcessor.class);

    private static Pattern alphaRegExPattern = Pattern.compile("[A-Z]*");

    private FastaSequenceReader fastaSequenceReader;

    public ContextNucleotideAdditionProcessor(FastaSequenceReader fastaReader) {
        this.fastaSequenceReader = fastaReader;
    }

    @Override
    public SubmittedVariantEntity process(SubmittedVariantEntity variant) throws Exception {
        String contig = variant.getContig();

        try {
            if (fastaSequenceReader.doesContigExist(contig)) {
                return getVariantWithContextNucleotide(variant);
            } else {
                throw new IllegalArgumentException("Contig '" + contig + "' does not appear in the FASTA file ");
            }
        } catch (PositionOutsideOfContigException e) {
            logger.warn(e.getMessage() + ". " + variant.toString());
            throw e;
        }
    }

    private SubmittedVariantEntity getVariantWithContextNucleotide(SubmittedVariantEntity variant) {
        long oldStart = variant.getStart();
        String contig = variant.getContig();
        String oldReference = variant.getReferenceAllele();
        String oldAlternate = variant.getAlternateAllele();

        VariantType variantType;
        try {
            variantType = getVariantType(oldReference, oldAlternate);
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Variant with strange alleles should have been filtered out by a previous processor.", e);
        }
        if (shouldBeRenormalized(variantType)) {
            return renormalizeIndel(variant, oldStart, contig, oldReference, oldAlternate);
        }
        return variant;
    }

    /**
     * Note that we don't consider VariantType.INDEL should be renormalized because we only want to avoid empty alleles.
     * An INDEL won't have any empty alleles and thus can be perfectly represented in VCF.
     * See 1.6.1.4 of VCFv4.3 (https://samtools.github.io/hts-specs/VCFv4.3.pdf)
     */
    private boolean shouldBeRenormalized(VariantType variantType) {
        return Arrays.asList(VariantType.INS, VariantType.DEL).contains(variantType);
    }

    /**
     * This function is enough for this scope but is incomplete for broader scopes!
     * It doesn't try to classify according to all the types (e.g. it misses SEQUENCE_VARIATION) and instead
     * it throws exceptions.
     */
    private VariantType getVariantType(String reference, String alternate) {
        String ref = reference.trim().toUpperCase();
        String alt = alternate.trim().toUpperCase();

        if (alphaRegExPattern.matcher(ref).matches() && alphaRegExPattern.matcher(alt).matches()) {
            if (ref.length() == alt.length()) {
                if (ref.length() == 1) {
                    return VariantType.SNV;
                } else if (ref.isEmpty()) {
                    throw new IllegalArgumentException("variant has both alleles empty");
                } else {
                    return VariantType.MNV;
                }
            } else {
                if (ref.isEmpty()) {
                    return VariantType.INS;
                } else if (alt.isEmpty()) {
                    return VariantType.DEL;
                } else {
                    return VariantType.INDEL;
                }
            }
        }

        throw new IllegalArgumentException(
                String.format("The VariantType of a Variant (with Reference: %s and Alternate: %s) is not one of "
                                      + "SNV, MNV, INS, DEL or INDEL.",
                              reference, alternate));
    }

    private SubmittedVariantEntity renormalizeIndel(SubmittedVariantEntity variant, long oldStart, String contig,
                                                    String oldReference, String oldAlternate) {
        String newReference;
        String newAlternate;
        ImmutableTriple<Long, String, String> contextNucleotideInfo =
                fastaSequenceReader.getContextNucleotideAndNewStart(contig, oldStart, oldReference, oldAlternate);

        long newStart = contextNucleotideInfo.getLeft();
        newReference = contextNucleotideInfo.getMiddle();
        newAlternate = contextNucleotideInfo.getRight();

        variant.setStart(newStart);
        variant.setReferenceAllele(newReference);
        variant.setAlternateAllele(newAlternate);
        return variant;
    }
}
