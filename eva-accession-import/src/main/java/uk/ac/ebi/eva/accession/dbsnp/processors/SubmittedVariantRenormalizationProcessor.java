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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

public class SubmittedVariantRenormalizationProcessor implements
        ItemProcessor<List<DbsnpSubmittedVariantEntity>, List<DbsnpSubmittedVariantEntity>> {

    private static final Logger logger = LoggerFactory.getLogger(SubmittedVariantRenormalizationProcessor.class);

    private FastaSequenceReader fastaSequenceReader;

    private Function<ISubmittedVariant, String> hashingFunction;

    public SubmittedVariantRenormalizationProcessor(FastaSequenceReader fastaSequenceReader) {
        this.fastaSequenceReader = fastaSequenceReader;
        hashingFunction = new DbsnpSubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Override
    public List<DbsnpSubmittedVariantEntity> process(List<DbsnpSubmittedVariantEntity> variants) {
        return variants.stream().map(this::process).collect(Collectors.toList());
    }

    private DbsnpSubmittedVariantEntity process(DbsnpSubmittedVariantEntity variant) {
        if (isAmbiguous(variant)) {
            return renormalize(variant);
        }
        return variant;
    }

    /**
     * We define the requirements to be an ambiguous variant as: being an indel (one allele is empty) and the non empty
     * allele ends with the same nucleotide as what is in the reference assembly right before the variant.
     *
     * The reason for this is because we know the variants being processed have removed the leftmost nucleotide as
     * redundant context, and we want it to have removed the rightmost nucleotide. This only yield different results
     * if the original context nucleotide appears in the beginning and end of the other allele.
     *
     * Examples: let's say in position 10 there's A, and an insertion happens between bases 10 and 11 of GGT.
     * This is represented as 11: "" > "GGT". This won't be ambiguous because T is not the same as A (last nucleotide !=
     * nucleotide before the variant). In contrast, if the insertion was "GGA" it is ambiguous. It would have appeared
     * originally as 10: "A" > "AGGA", which can be interpreted as 11: "" > "GGA" (variant being processed) or as
     * 10: "" > "AGG" (desired representation). For deletions the explanation is the same.
     *
     * For the needed change,
     * @see SubmittedVariantRenormalizationProcessor#renormalize(uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity)
     * @see SubmittedVariantRenormalizationProcessor#renormalizeAllele(java.lang.String)
     */
    private boolean isAmbiguous(ISubmittedVariant variant) {
        try {
            boolean isIndel = variant.getReferenceAllele().length() != variant.getAlternateAllele().length();
            boolean oneAlleleIsEmpty = variant.getReferenceAllele().isEmpty() ^ variant.getAlternateAllele().isEmpty();
            return isIndel && oneAlleleIsEmpty && areContextAndLastNucleotideEqual(variant);
        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            // if something went wrong with the fasta, we can not say it's ambiguous
            return false;
        }
    }

    private boolean areContextAndLastNucleotideEqual(ISubmittedVariant variant) {
        String nonEmptyAllele = variant.getReferenceAllele().isEmpty() ? variant.getAlternateAllele() :
                variant.getReferenceAllele();
        char lastNucleotideInAllele = nonEmptyAllele.charAt(nonEmptyAllele.length() - 1);
        char contextBaseInAssembly = getContextBaseInAssembly(variant);

        return lastNucleotideInAllele == contextBaseInAssembly;
    }

    private char getContextBaseInAssembly(ISubmittedVariant variant) {
        long contextPosition = variant.getStart() - 1;
        String sequence = fastaSequenceReader.getSequence(variant.getContig(), contextPosition, contextPosition);
        if (sequence == null || sequence.length() != 1) {
            throw new RuntimeException(
                    "Reference sequence could not be retrieved correctly for chromosome=\"" + variant.getContig()
                            + "\", position=" + contextPosition);
        }
        char contextBaseInAssembly = sequence.charAt(0);
        return contextBaseInAssembly;
    }

    private DbsnpSubmittedVariantEntity renormalize(DbsnpSubmittedVariantEntity variant) {
        String renormalizedReference = variant.getReferenceAllele();
        String renormalizedAlternate = variant.getAlternateAllele();
        long renormalizedStart = variant.getStart() - 1;

        if (variant.getReferenceAllele().isEmpty()) {
            renormalizedAlternate = renormalizeAllele(variant.getAlternateAllele());
        } else if (variant.getAlternateAllele().isEmpty()) {
            renormalizedReference = renormalizeAllele(variant.getReferenceAllele());
        } else {
            throw new AssertionError("Can not re-normalize due to non-standard INDEL: " + variant);
        }

        return createNormalizedVariant(variant, renormalizedAlternate, renormalizedReference, renormalizedStart);
    }

    /**
     * Just move the last nucleotide of the allele to the first position
     */
    private String renormalizeAllele(String allele) {
        int length = allele.length();
        char nucleotideThatShouldBeAtTheBeginning = allele.charAt(length - 1);
        String alleleWithoutLastNucleotide = allele.substring(0, length - 1);
        String renormalizedAllele = nucleotideThatShouldBeAtTheBeginning + alleleWithoutLastNucleotide;
        return renormalizedAllele;
    }

    private DbsnpSubmittedVariantEntity createNormalizedVariant(DbsnpSubmittedVariantEntity variant,
                                                                String renormalizedAlternate,
                                                                String renormalizedReference, long renormalizedStart) {
        ISubmittedVariant normalizedVariantModel = new SubmittedVariant(variant.getAssemblyAccession(),
                                                                        variant.getTaxonomyAccession(),
                                                                        variant.getProjectAccession(),
                                                                        variant.getContig(), renormalizedStart,
                                                                        renormalizedReference, renormalizedAlternate,
                                                                        variant.getClusteredVariantAccession(),
                                                                        variant.isSupportedByEvidence(),
                                                                        variant.isAssemblyMatch(),
                                                                        variant.isAllelesMatch(),
                                                                        variant.isValidated());
        return new DbsnpSubmittedVariantEntity(variant.getAccession(), hashingFunction.apply(normalizedVariantModel),
                                               normalizedVariantModel, variant.getVersion());

    }


}
