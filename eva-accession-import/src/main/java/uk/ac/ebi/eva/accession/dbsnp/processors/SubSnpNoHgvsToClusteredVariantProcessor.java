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

    @Override
    public DbsnpClusteredVariantEntity process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        Region variantRegion = subSnpNoHgvs.getVariantRegion();
        List<String> alleles = subSnpNoHgvs.getAlternateAllelesInForwardStrand();
        VariantType type = VariantClassifier.getVariantClassification(subSnpNoHgvs.getReference(),
                                                                      alleles.get(0), // TODO which allele to use? we
                                                                      // need the logic of the declustering to
                                                                      // complete this
                                                                      subSnpNoHgvs.getDbsnpClass().intValue());
        ClusteredVariant variant = new ClusteredVariant(subSnpNoHgvs.getAssembly(),
                                                        subSnpNoHgvs.getTaxonomyId(),
                                                        variantRegion.getChromosome(),
                                                        variantRegion.getStart(),
                                                        type,
                                                        subSnpNoHgvs.isSnpValidated());

        String hash = hashingFunction.apply(variant);

        DbsnpClusteredVariantEntity variantEntity = new DbsnpClusteredVariantEntity(subSnpNoHgvs.getRsId(),
                                                                                    hash,
                                                                                    variant);

        variantEntity.setCreatedDate(subSnpNoHgvs.getRsCreateTime().toLocalDateTime());
        return variantEntity;
    }

}
