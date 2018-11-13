package uk.ac.ebi.eva.accession.release.steps.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Filters variants with invalid alleles.
 *
 * VCF format only accepts reference and alternate alleles formed by A, C, G, T or N letters in upper or lower case.
 * If one of the alleles has a different character, this processor will return null so that variant can be ignored.
 */
public class ExcludeInvalidVariantsProcessor implements ItemProcessor<Variant, Variant> {

    private static final String ALLELES_REGEX = "^[acgtnACGTN]+$";

    private static final Pattern ALLELES_PATTERN = Pattern.compile(ALLELES_REGEX);

    static final String REFERENCE_AND_ALTERNATE_ALLELES_CANNOT_BE_EMPTY =
            "Neither the reference nor the alternate allele should be empty.";

    @Override
    public Variant process(Variant variant) throws Exception {
        if (variant.getReference().isEmpty() || variant.getAlternate().isEmpty()) {
            throw new IllegalArgumentException(REFERENCE_AND_ALTERNATE_ALLELES_CANNOT_BE_EMPTY);
        }

        Matcher matcher = ALLELES_PATTERN.matcher(variant.getReference() + variant.getAlternate());
        if(matcher.matches()) {
            return variant;
        }
        return null;
    }

}
