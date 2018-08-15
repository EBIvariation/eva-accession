/*
 * Copyright 2014-2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.summary.DbsnpClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.model.DbsnpVariantType;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.commons.core.models.Region;
import uk.ac.ebi.eva.commons.core.models.VariantClassifier;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.List;
import java.util.function.Function;

public class SubSnpNoHgvsToClusteredVariantProcessor
        implements ItemProcessor<SubSnpNoHgvs, DbsnpClusteredVariantEntity> {

    private Function<IClusteredVariant, String> hashingFunction;

    public SubSnpNoHgvsToClusteredVariantProcessor() {
        hashingFunction = new DbsnpClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    /**
     * Instantiate a {@link DbsnpClusteredVariantEntity} from a {@link SubSnpNoHgvs}.
     *
     * We define the type of the ClusteredVariant (RefSnp) depending on the dbsnp type. If the dbsnp type is DIV then
     * it would depend on the first SubSnp having any of these types: INS, DEL or INDEL (according to
     * {@link VariantClassifier}).
     *
     * If the dbsnp type is different from DIV it will be directly mapped to the corresponding type.
     *
     * If the alternate alleles represent different variant types, the {@link DbsnpClusteredVariantEntity} of any
     * different type will be declustered in {@link SubSnpNoHgvsToDbsnpVariantsWrapperProcessor}.
     */
    @Override
    public DbsnpClusteredVariantEntity process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        Region variantRegion = subSnpNoHgvs.getVariantRegion();

        VariantType variantType;
        if (subSnpNoHgvs.getDbsnpVariantType() != DbsnpVariantType.DIV) {
            variantType = getVariantType(subSnpNoHgvs);
        } else {
            variantType = getVariantTypeWhenDiv(subSnpNoHgvs);
        }

        ClusteredVariant variant = new ClusteredVariant(subSnpNoHgvs.getAssembly(),
                                                        subSnpNoHgvs.getTaxonomyId(),
                                                        variantRegion.getChromosome(),
                                                        variantRegion.getStart(),
                                                        variantType,
                                                        subSnpNoHgvs.isSnpValidated());

        String hash = hashingFunction.apply(variant);

        DbsnpClusteredVariantEntity variantEntity = new DbsnpClusteredVariantEntity(subSnpNoHgvs.getRsId(),
                                                                                    hash,
                                                                                    variant);

        variantEntity.setCreatedDate(subSnpNoHgvs.getRsCreateTime().toLocalDateTime());
        return variantEntity;
    }

    private VariantType getVariantType(SubSnpNoHgvs subSnpNoHgvs) {
        switch (subSnpNoHgvs.getDbsnpVariantType()) {
            case SNV:
                return VariantType.SNV;
            case MICROSATELLITE:
                return VariantType.TANDEM_REPEAT;
            case NAMED:
                return VariantType.SEQUENCE_ALTERATION;
            case NO_VARIATION:
                return VariantType.NO_SEQUENCE_ALTERATION;
            case MNV:
                return VariantType.MNV;
            default:
                throw new IllegalArgumentException(
                        "The dbSNP variant type provided doesn't have a direct mapping to an EVA type");
        }
    }

    /**
     * If the RefSnp type cannot be determined because none of the associated SubSnps are type INS, DEL or INDEL, the
     * type INDEL is assigned by default, and the SubSnps should be declustered by
     * {@link SubSnpNoHgvsToDbsnpVariantsWrapperProcessor}.
     */
    private VariantType getVariantTypeWhenDiv(SubSnpNoHgvs subSnpNoHgvs) {
        List<String> alternateAlleles = subSnpNoHgvs.getAlternateAllelesInForwardStrand();
        for (String alternateAllele : alternateAlleles) {
            VariantType ssVariantType = VariantClassifier.getVariantClassification(
                    subSnpNoHgvs.getReferenceInForwardStrand(),
                    alternateAllele,
                    subSnpNoHgvs.getDbsnpVariantType().intValue());

            if (ssVariantType == VariantType.INS || ssVariantType == VariantType.DEL ||
                    ssVariantType == VariantType.INDEL) {
                return ssVariantType;
            }
        }
        return VariantType.INDEL;
    }

}
