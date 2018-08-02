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
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.io.FastaSequenceReader;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.summary.DbsnpSubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.commons.core.models.Region;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;

import java.util.List;

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
        List<DbsnpSubmittedVariantEntity> variants = new ArrayList<>();
        List<DbsnpSubmittedVariantOperationEntity> operations = new ArrayList<>();
        DbsnpClusteredVariantEntity clusteredVariant = subSnpNoHgvsToClusteredVariantProcessor.process(subSnpNoHgvs);

        List<String> alternateAlleles = subSnpNoHgvs.getAlternateAllelesInForwardStrand();
        for (String alternateAllele : alternateAlleles) {
            SubmittedVariant variant = subSnpNoHgvsToSubmittedVariant(subSnpNoHgvs, alternateAllele);
            if (!variant.isAllelesMatch()) {
                decluster(subSnpNoHgvs.getSsId(), variant, operations);
            }

            String hash = hashingFunction.apply(variant);
            DbsnpSubmittedVariantEntity ssVariantEntity = new DbsnpSubmittedVariantEntity(subSnpNoHgvs.getSsId(), hash,
                                                                                          variant);
            ssVariantEntity.setCreatedDate(subSnpNoHgvs.getSsCreateTime().toLocalDateTime());
            variants.add(ssVariantEntity);
        }

        List<DbsnpSubmittedVariantEntity> normalisedSubmittedVariants = renormalizationProcessor.process(variants);

        dbsnpVariantsWrapper.setDbsnpVariantType(subSnpNoHgvs.getDbsnpVariantType());
        dbsnpVariantsWrapper.setClusteredVariant(clusteredVariant);
        dbsnpVariantsWrapper.setSubmittedVariants(normalisedSubmittedVariants);
        dbsnpVariantsWrapper.setOperations(operations);
        return dbsnpVariantsWrapper;
    }

    private SubmittedVariant subSnpNoHgvsToSubmittedVariant(SubSnpNoHgvs subSnpNoHgvs, String alternate) {
        Region variantRegion = getVariantRegion(subSnpNoHgvs);
        String reference = subSnpNoHgvs.getReferenceInForwardStrand();
        SubmittedVariant variant = new SubmittedVariant(subSnpNoHgvs.getAssembly(), subSnpNoHgvs.getTaxonomyId(),
                                                        getProjectAccession(subSnpNoHgvs),
                                                        variantRegion.getChromosome(), variantRegion.getStart(),
                                                        reference, alternate, subSnpNoHgvs.getRsId());
        variant.setSupportedByEvidence(subSnpNoHgvs.isFrequencyExists() || subSnpNoHgvs.isGenotypeExists());
        variant.setAllelesMatch(subSnpNoHgvs.doAllelesMatch());
        variant.setValidated(subSnpNoHgvs.isSubsnpValidated());

        return variant;
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

    private void decluster(Long ss, SubmittedVariant variant, List<DbsnpSubmittedVariantOperationEntity> operations) {
        //Register decluster operation
        DbsnpSubmittedVariantEntity ssVariantEntityNotDeclusteredVariant =
                new DbsnpSubmittedVariantEntity(ss, hashingFunction.apply(variant), variant);
        DbsnpSubmittedVariantOperationEntity operation = new DbsnpSubmittedVariantOperationEntity();
        DbsnpSubmittedVariantInactiveEntity inactiveEntity =
                new DbsnpSubmittedVariantInactiveEntity(ssVariantEntityNotDeclusteredVariant);
        operation.fill(EventType.UPDATED, ss, null, "Declustered (Alleles mismatch)",
                       Collections.singletonList(inactiveEntity));
        operations.add(operation);
        //Decluster SS
        variant.setClusteredVariantAccession(null);
    }
}
