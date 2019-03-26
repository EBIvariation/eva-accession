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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.commons.core.models.AbstractVariantSourceEntry;
import uk.ac.ebi.eva.commons.core.models.IVariant;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.VariantTypeToSOAccessionMap;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

import java.util.Arrays;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static uk.ac.ebi.eva.accession.release.io.VariantMongoAggregationReader.VARIANT_CLASS_KEY;

/**
 * Transform a named variant {@link VariantType#SEQUENCE_ALTERATION} into a structural variant with symbolic alleles.
 *
 * Examples of structural variants taken from
 * <a href='https://samtools.github.io/hts-specs/VCFv4.3.pdf'>VCFv.3 spec</a> (section 5.3):
 * <pre>
 * {@code
 * #CHROM POS      ID   REF  ALT          QUAL FILTER INFO
 * 2        321682 .    T    <DEL>        6    PASS   SVTYPE=DEL;END=321887;SVLEN=-205
 * 2      14477084 .    C    <DEL:ME:ALU> 12   PASS   SVTYPE=DEL;END=14477381;SVLEN=-297
 * 3       9425916 .    C    <INS:ME:L1>  23   PASS   SVTYPE=INS;END=9425916;SVLEN=6027
 * }
 * </pre>
 *
 * Note that unlike regular INDELS, variants with symbolic alleles have the context bases only in the REF column.
 *
 * Also, note that the deletions have the symbolic allele in the ALT column.
 */
public class NamedVariantProcessor implements ItemProcessor<Variant, IVariant> {

    private static Logger logger = LoggerFactory.getLogger(NamedVariantProcessor.class);

    private static Map<String, VariantType> sequenceOntologyToVariantType =
            Arrays.stream(VariantType.values())
                  .filter(type -> type != VariantType.NO_ALTERNATE)
                  .collect(Collectors.toMap(VariantTypeToSOAccessionMap::getSequenceOntologyAccession,
                                            type -> type));

    @Override
    public IVariant process(Variant variant) throws Exception {

        String oldReference = variant.getReference();
        String oldAlternate = variant.getAlternate();

        String newReference = oldReference;
        String newAlternate = oldAlternate;

        if (isNamedAllele(oldReference) || isSymbolicAllele(oldReference)) {
            if (isNamedAllele(oldAlternate) || isSymbolicAllele(oldAlternate)) {
                throw new IllegalArgumentException(
                        "This variant (with named/symbolic alleles in both the reference and alternate alleles) can't"
                        + " be written in VCF, as only the ALT column can have symbolic alleles: " + variant);
            } else {
                // swap the alleles, look this class' documentation
                newReference = oldAlternate;
                newAlternate = oldReference;
            }
        } else {
            // normal case without special reference allele: ok as it is
        }

        Variant newVariant = new Variant(variant.getChromosome(), variant.getStart(), variant.getEnd(), newReference,
                                         makeAltAlleleValid(newAlternate));

        newVariant.addSourceEntries(variant.getSourceEntries());
        newVariant.setMainId(variant.getMainId());

        logIfVariantHasWrongType(newVariant);

        return newVariant;
    }

    /**
     * Named variants have alleles surrounded by parentheses and symbolic alleles are surrounded by angle brackets.
     * In order to represent those alleles in VCF format (as symbolic alleles in the ALT allele column), the surrounding
     * characters will be replaced by angular brackets, and invalid characters (space, comma, angular brackets) within
     * the id itself will be replaced by underscore.
     */
    private String makeAltAlleleValid(String allele) {
        if (isNamedAllele(allele) || isSymbolicAllele(allele)) {
            String potentiallyInvalidSymbolicId = getAlleleIdWithoutSurroundingBraces(allele);
            String validSymbolicId = potentiallyInvalidSymbolicId.replaceAll("[ ,<>]", "_");
            return "<" + validSymbolicId + ">";
        } else {
            return allele;
        }
    }

    private boolean isNamedAllele(String allele) {
        return allele.startsWith("(") && allele.endsWith(")");
    }

    private boolean isSymbolicAllele(String allele) {
        return allele.startsWith("<") && allele.endsWith(">");
    }

    private String getAlleleIdWithoutSurroundingBraces(String allele) {
        return allele.substring(1, allele.length() - 1);
    }

    private void logIfVariantHasWrongType(Variant variant) {
        Set<VariantType> types = getTypeFromAttributes(variant);
        boolean containsSequenceAlterationInMongo = types.contains(VariantType.SEQUENCE_ALTERATION);
        boolean isSequenceAlteration = variant.getType() == VariantType.SEQUENCE_ALTERATION;

        if (isSequenceAlteration && !containsSequenceAlterationInMongo
            || !isSequenceAlteration && containsSequenceAlterationInMongo) {
            logger.warn("Variant is stored in MongoDB with types " + types + ", but AbstractVariant::getType says"
                        + " that the correct type is " + variant.getType() + ". The variant (" + variant.getMainId()
                        + ") is: " + variant);
        }
    }

    private Set<VariantType> getTypeFromAttributes(Variant variant) {
        return variant.getSourceEntries()
                      .stream()
                      .map(AbstractVariantSourceEntry::getAttributes)
                      .map(attributes -> attributes.get(VARIANT_CLASS_KEY))
                      .map(sequenceOntologyToVariantType::get)
                      .collect(Collectors.toSet());
    }
}
