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

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.commons.core.models.Region;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SubSnpNoHgvsToSubmittedVariantProcessor
        implements ItemProcessor<SubSnpNoHgvs, List<DbsnpSubmittedVariantEntity>> {

    private Function<ISubmittedVariant, String> hashingFunction;

    public SubSnpNoHgvsToSubmittedVariantProcessor() {
        hashingFunction = new DbsnpSubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Override
    public List<DbsnpSubmittedVariantEntity> process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        List<DbsnpSubmittedVariantEntity> variants = new ArrayList<>();

        Region variantRegion = getVariantRegion(subSnpNoHgvs);

        String reference = subSnpNoHgvs.getReferenceInForwardStrand();
        boolean allelesMatch = subSnpNoHgvs.doAllelesMatch();
        List<String> alternateAlleles = subSnpNoHgvs.getAlternateAllelesInForwardStrand();
        for (String alternate : alternateAlleles) {
            // a SubmittedVariant object is needed to calculate the hash to create the DbsnpSubmittedVariantEntity one
            SubmittedVariant variant = new SubmittedVariant(subSnpNoHgvs.getAssembly(), subSnpNoHgvs.getTaxonomyId(),
                                                            getProjectAccession(subSnpNoHgvs),
                                                            variantRegion.getChromosome(), variantRegion.getStart(),
                                                            reference, alternate, subSnpNoHgvs.getRsId());
            variant.setSupportedByEvidence(subSnpNoHgvs.isFrequencyExists() || subSnpNoHgvs.isGenotypeExists());
            variant.setAllelesMatch(allelesMatch);
            variant.setValidated(subSnpNoHgvs.isSubsnpValidated());

            String hash = hashingFunction.apply(variant);
            DbsnpSubmittedVariantEntity ssVariantEntity = new DbsnpSubmittedVariantEntity(subSnpNoHgvs.getSsId(), hash,
                                                                                          variant);
            ssVariantEntity.setCreatedDate(subSnpNoHgvs.getSsCreateTime().toLocalDateTime());
            variants.add(ssVariantEntity);
        }

        return variants;
    }

    private Region getVariantRegion(SubSnpNoHgvs subSnpNoHgvs) {
        if (subSnpNoHgvs.getChromosome() != null) {
            return new Region(subSnpNoHgvs.getChromosome(), subSnpNoHgvs.getChromosomeStart());
        } else {
            return new Region(subSnpNoHgvs.getContigName(), subSnpNoHgvs.getContigStart());
        }
    }

    private String getProjectAccession(SubSnpNoHgvs subSnpNoHgvs) {
        return subSnpNoHgvs.getBatchHandle() + "_" + subSnpNoHgvs.getBatchName();
    }
}
