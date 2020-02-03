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

package uk.ac.ebi.eva.accession.release.batch.processors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.core.models.VariantType;
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

    private static final Logger logger = LoggerFactory.getLogger(ExcludeInvalidVariantsProcessor.class);

    private static final String ALLELES_REGEX = "^[acgtnACGTN]+$";

    private static final Pattern ALLELES_PATTERN = Pattern.compile(ALLELES_REGEX);

    static final String REFERENCE_AND_ALTERNATE_ALLELES_CANNOT_BE_EMPTY =
            "Neither the reference nor the alternate allele should be empty.";

    @Override
    public Variant process(Variant variant) throws Exception {
        if (variant.getType() == VariantType.SEQUENCE_ALTERATION) {
            return variant;
        }

        Matcher matcher = ALLELES_PATTERN.matcher(variant.getReference() + variant.getAlternate());
        if(matcher.matches()) {
            return variant;
        }
        logger.warn("Variant {} excluded (it has non-nucleotide letters and it's not a named variant)", variant);
        return null;
    }
}
