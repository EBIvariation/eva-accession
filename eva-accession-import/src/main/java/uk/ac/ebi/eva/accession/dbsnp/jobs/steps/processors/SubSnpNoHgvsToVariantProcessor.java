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
package uk.ac.ebi.eva.accession.dbsnp.jobs.steps.processors;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.Region;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

public class SubSnpNoHgvsToVariantProcessor implements ItemProcessor<SubSnpNoHgvs, List<DbsnpSubmittedVariantEntity>> {

    private Function<ISubmittedVariant, String> hashingFunction;

    public SubSnpNoHgvsToVariantProcessor() {
        hashingFunction = new DbsnpSubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Override
    public List<DbsnpSubmittedVariantEntity> process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        List<DbsnpSubmittedVariantEntity> variants = new ArrayList<>();

        Region variantRegion = getVariantRegion(subSnpNoHgvs);

        List<String> alternateAlleles = subSnpNoHgvs.getAlternateAllelesInForwardStrand();
        for (String alternate : alternateAlleles) {
            // a ISubmittedVariant is needed to calculate the hash to create the DbsnpSubmittedVariantEntity object
            SubmittedVariant variant = new SubmittedVariant(subSnpNoHgvs.getAssembly(), subSnpNoHgvs.getTaxonomyId(),
                                                             getProjectAccession(subSnpNoHgvs),
                                                             variantRegion.getChromosome(), variantRegion.getStart(),
                                                             subSnpNoHgvs.getReferenceInForwardStrand(), alternate,
                                                             subSnpNoHgvs.getRsId());
            variant.setSupportedByEvidence(subSnpNoHgvs.isFrequencyExists() || subSnpNoHgvs.isGenotypeExists());
            String hash = hashingFunction.apply(variant);

            DbsnpSubmittedVariantEntity ssVariant = new DbsnpSubmittedVariantEntity(subSnpNoHgvs.getSsId(), hash,
                                                                                    variant);
            variants.add(ssVariant);
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
