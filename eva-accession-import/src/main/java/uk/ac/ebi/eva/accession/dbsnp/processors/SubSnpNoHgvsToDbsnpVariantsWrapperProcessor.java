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
package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.commons.core.models.Region;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;

import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_ALLELES_MATCH;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_SUPPORTED_BY_EVIDENCE;
import static uk.ac.ebi.eva.accession.core.ISubmittedVariant.DEFAULT_VALIDATED;

public class SubSnpNoHgvsToDbsnpVariantsWrapperProcessor implements ItemProcessor<SubSnpNoHgvs, DbsnpVariantsWrapper> {

    private SubmittedVariantRenormalizationProcessor renormalizationProcessor;

    private SubSnpNoHgvsToClusteredVariantProcessor subSnpNoHgvsToClusteredVariantProcessor;

    private Function<ISubmittedVariant, String> hashingFunction;

    public SubSnpNoHgvsToDbsnpVariantsWrapperProcessor(FastaSequenceReader fastaSequenceReader) {
        renormalizationProcessor = new SubmittedVariantRenormalizationProcessor(fastaSequenceReader);
        subSnpNoHgvsToClusteredVariantProcessor = new SubSnpNoHgvsToClusteredVariantProcessor();
        hashingFunction = new DbsnpSubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
    }

    @Override
    public DbsnpVariantsWrapper process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        DbsnpVariantsWrapper dbsnpVariantsWrapper = new DbsnpVariantsWrapper();
        List<DbsnpSubmittedVariantEntity> submittedVariants = new ArrayList<>();
        DbsnpClusteredVariantEntity clusteredVariant = subSnpNoHgvsToClusteredVariantProcessor.process(subSnpNoHgvs);

        List<String> alternateAlleles = subSnpNoHgvs.getAlternateAllelesInForwardStrand();
        for (String alternateAllele : alternateAlleles) {
            SubmittedVariant submittedVariant = subSnpNoHgvsToSubmittedVariant(subSnpNoHgvs, alternateAllele);
            addSubmittedVariantEntity(subSnpNoHgvs, submittedVariant, submittedVariants);
        }

        List<DbsnpSubmittedVariantEntity> normalisedSubmittedVariants =
                renormalizationProcessor.process(submittedVariants);

        dbsnpVariantsWrapper.setDbsnpVariantType(subSnpNoHgvs.getDbsnpVariantType());
        dbsnpVariantsWrapper.setClusteredVariant(clusteredVariant);
        dbsnpVariantsWrapper.setSubmittedVariants(normalisedSubmittedVariants);
        return dbsnpVariantsWrapper;
    }

    private void addSubmittedVariantEntity(SubSnpNoHgvs subSnpNoHgvs,
                                           SubmittedVariant submittedVariant,
                                           List<DbsnpSubmittedVariantEntity> submittedVariants) {
        String hash = hashingFunction.apply(submittedVariant);
        DbsnpSubmittedVariantEntity submittedVariantEntity = new DbsnpSubmittedVariantEntity(subSnpNoHgvs.getSsId(),
                                                                                             hash, submittedVariant,
                                                                                             1);
        submittedVariantEntity.setCreatedDate(getCreatedDate(subSnpNoHgvs));
        submittedVariants.add(submittedVariantEntity);
    }

    private SubmittedVariant subSnpNoHgvsToSubmittedVariant(SubSnpNoHgvs subSnpNoHgvs, String alternate) {
        Region variantRegion = subSnpNoHgvs.getVariantRegion();
        String reference = subSnpNoHgvs.getReferenceInForwardStrand();
        /*
            assemblyMatch is set to false because null is not allowed but the assembly checker should determine the
            real value of assemblyMatch.
         */
        SubmittedVariant variant = new SubmittedVariant(subSnpNoHgvs.getAssembly(), subSnpNoHgvs.getTaxonomyId(),
                                                        getProjectAccession(subSnpNoHgvs),
                                                        variantRegion.getChromosome(), variantRegion.getStart(),
                                                        reference, alternate, subSnpNoHgvs.getRsId(),
                                                        DEFAULT_SUPPORTED_BY_EVIDENCE, false, DEFAULT_ALLELES_MATCH,
                                                        DEFAULT_VALIDATED);
        variant.setSupportedByEvidence(subSnpNoHgvs.isFrequencyExists() || subSnpNoHgvs.isGenotypeExists());
        variant.setAllelesMatch(subSnpNoHgvs.doAllelesMatch());
        variant.setValidated(subSnpNoHgvs.isSubsnpValidated());

        return variant;
    }

    private String getProjectAccession(SubSnpNoHgvs subSnpNoHgvs) {
        return subSnpNoHgvs.getBatchHandle() + "_" + subSnpNoHgvs.getBatchName();
    }

    private LocalDateTime getCreatedDate(SubSnpNoHgvs subSnpNoHgvs) {
        LocalDateTime createdDate;
        if ((createdDate = subSnpNoHgvs.getSsCreateTime().toLocalDateTime()) != null) {
            return createdDate;
        } else if ((createdDate = subSnpNoHgvs.getRsCreateTime().toLocalDateTime()) != null) {
            return createdDate;
        } else {
            return LocalDateTime.now();
        }
    }
}
