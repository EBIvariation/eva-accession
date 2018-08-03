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

import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;

import java.util.List;

public class SubSnpNoHgvsToDbsnpVariantsWrapperProcessor implements ItemProcessor<SubSnpNoHgvs, DbsnpVariantsWrapper> {

    private SubSnpNoHgvsToSubmittedVariantProcessor subSnpNoHgvsToSubmittedVariantProcessor;

    private SubmittedVariantRenormalizationProcessor renormalizationProcessor;

    private SubSnpNoHgvsToClusteredVariantProcessor subSnpNoHgvsToClusteredVariantProcessor;

    public SubSnpNoHgvsToDbsnpVariantsWrapperProcessor(FastaSequenceReader fastaSequenceReader) {
        subSnpNoHgvsToSubmittedVariantProcessor = new SubSnpNoHgvsToSubmittedVariantProcessor();
        renormalizationProcessor = new SubmittedVariantRenormalizationProcessor(fastaSequenceReader);
        subSnpNoHgvsToClusteredVariantProcessor = new SubSnpNoHgvsToClusteredVariantProcessor();
    }

    @Override
    public DbsnpVariantsWrapper process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        DbsnpVariantsWrapper dbsnpVariantsWrapper = new DbsnpVariantsWrapper();

        // SS
        List<DbsnpSubmittedVariantEntity> submittedVariants = subSnpNoHgvsToSubmittedVariantProcessor.process(
                subSnpNoHgvs);
        List<DbsnpSubmittedVariantEntity> normalisedSubmittedVariants = renormalizationProcessor.process(
                submittedVariants);
        dbsnpVariantsWrapper.setSubmittedVariants(normalisedSubmittedVariants);

        // RS
        DbsnpClusteredVariantEntity clusteredVariant = subSnpNoHgvsToClusteredVariantProcessor.process(subSnpNoHgvs);
        dbsnpVariantsWrapper.setClusteredVariant(clusteredVariant);

        // TODO create operations
        return dbsnpVariantsWrapper;
    }
}
