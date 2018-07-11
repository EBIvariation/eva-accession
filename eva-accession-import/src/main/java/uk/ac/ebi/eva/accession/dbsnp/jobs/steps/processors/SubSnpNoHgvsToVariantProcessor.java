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

import java.util.function.Function;

public class SubSnpNoHgvsToVariantProcessor implements ItemProcessor<SubSnpNoHgvs, DbsnpSubmittedVariantEntity> {

    private Function<ISubmittedVariant, String> hashingFunction;

    public SubSnpNoHgvsToVariantProcessor() {
        hashingFunction = new DbsnpSubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Override
    public DbsnpSubmittedVariantEntity process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        // this method will return the chromosome region or the contig one if there is no chromosome mapping
        Region variantRegion = getVariantRegion(subSnpNoHgvs);

        // a ISubmittedVariant is needed to calculate the hash to create the DbsnpSubmittedVariantEntity object
        ISubmittedVariant variant = new SubmittedVariant(subSnpNoHgvs.getAssembly(), subSnpNoHgvs.getTaxonomyId(),
                                                         subSnpNoHgvs.getBatchHandle() + "_" + subSnpNoHgvs
                                                                 .getBatchName(), variantRegion.getChromosome(),
                                                         variantRegion.getStart(),
                                                         subSnpNoHgvs.getReferenceInForwardStrand(),
                                                         subSnpNoHgvs.getAlternateInForwardStrand(),
                                                         subSnpNoHgvs.getRsId(),
                                                         subSnpNoHgvs.isFrequencyExists() || subSnpNoHgvs
                                                                 .isGenotypeExists(), false, false, false);
        String hash = hashingFunction.apply(variant);

        return new DbsnpSubmittedVariantEntity(subSnpNoHgvs.getSsId(), hash, variant);
    }

    private Region getVariantRegion(SubSnpNoHgvs subSnpNoHgvs) {
        if (subSnpNoHgvs.getChromosomeRegion() != null) {
            return subSnpNoHgvs.getChromosomeRegion();
        } else {
            return subSnpNoHgvs.getContigRegion();
        }
    }
}
