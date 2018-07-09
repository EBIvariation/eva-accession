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

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.commons.core.models.Region;

public class SubSnpNoHgvsToVariantProcessor implements ItemProcessor<SubSnpNoHgvs, ISubmittedVariant> {

    @Override
    public ISubmittedVariant process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        Region variantRegion = getVariantRegion(subSnpNoHgvs);
        return new SubmittedVariant(subSnpNoHgvs.getAssembly(), subSnpNoHgvs.getTaxonomyId(),
                                    subSnpNoHgvs.getBatchHandle() + "_" + subSnpNoHgvs.getBatchName(),
                                    variantRegion.getChromosome(), variantRegion.getStart(),
                                    subSnpNoHgvs.getReference(), subSnpNoHgvs.getAlternate(),
                                    subSnpNoHgvs.isFrequencyExists() || subSnpNoHgvs.isGenotypeExists());
    }

    private Region getVariantRegion(SubSnpNoHgvs subSnpNoHgvs) {
        if (subSnpNoHgvs.getChromosomeRegion() != null) {
            return subSnpNoHgvs.getChromosomeRegion();
        } else {
            return subSnpNoHgvs.getContigRegion();
        }
    }
}
