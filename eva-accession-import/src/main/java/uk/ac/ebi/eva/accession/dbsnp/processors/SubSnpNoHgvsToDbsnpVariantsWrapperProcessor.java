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
import uk.ac.ebi.eva.accession.core.persistence.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.dbsnp.io.FastaSynonymSequenceReader;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;
import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.dbsnp.persistence.DbsnpVariantsWrapper;
import uk.ac.ebi.eva.accession.dbsnp.persistence.ProjectAccessionMapping;
import uk.ac.ebi.eva.commons.core.models.Region;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;

public class SubSnpNoHgvsToDbsnpVariantsWrapperProcessor implements ItemProcessor<SubSnpNoHgvs, DbsnpVariantsWrapper> {

    private SubmittedVariantRenormalizationProcessor renormalizationProcessor;

    private SubSnpNoHgvsToClusteredVariantProcessor subSnpNoHgvsToClusteredVariantProcessor;

    private Function<ISubmittedVariant, String> hashingFunction;

    private String assemblyAccession;

    private List<ProjectAccessionMapping> projectAccessionMappings;

    public SubSnpNoHgvsToDbsnpVariantsWrapperProcessor(String assemblyAccession,
                                                       FastaSynonymSequenceReader fastaSequenceReader,
                                                       List<ProjectAccessionMapping> projectAccessionMappings) {
        this.assemblyAccession = assemblyAccession;
        this.renormalizationProcessor = new SubmittedVariantRenormalizationProcessor(fastaSequenceReader);
        this.subSnpNoHgvsToClusteredVariantProcessor = new SubSnpNoHgvsToClusteredVariantProcessor(assemblyAccession);
        this.hashingFunction = new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());
        this.projectAccessionMappings = projectAccessionMappings;
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
                                                                                             hash, submittedVariant, 1);
        submittedVariants.add(submittedVariantEntity);
    }

    private SubmittedVariant subSnpNoHgvsToSubmittedVariant(SubSnpNoHgvs subSnpNoHgvs, String alternate) {
        Region variantRegion = subSnpNoHgvs.getVariantRegion();
        return new SubmittedVariant(this.assemblyAccession, subSnpNoHgvs.getTaxonomyId(),
                                    getProjectAccession(subSnpNoHgvs), variantRegion.getChromosome(),
                                    variantRegion.getStart(), subSnpNoHgvs.getReferenceInForwardStrand(), alternate,
                                    subSnpNoHgvs.getRsId(),
                                    subSnpNoHgvs.isFrequencyExists() || subSnpNoHgvs.isGenotypeExists(),
                                    subSnpNoHgvs.isAssemblyMatch() ,
                                    subSnpNoHgvs.doAllelesMatch() && !subSnpNoHgvs.isAnyOrientationUnknown(),
                                    subSnpNoHgvs.isSubsnpValidated(),
                                    getCreatedDate(subSnpNoHgvs));
    }

    private String getProjectAccession(SubSnpNoHgvs subSnpNoHgvs) {
        Optional<String> studyId = projectAccessionMappings.stream()
                                                           .filter(sm -> sm.getDbsnpBatchHandle()
                                                                           .equals(subSnpNoHgvs.getBatchHandle()))
                                                           .filter(sm -> sm.getDbsnpBatchName()
                                                                           .equals(subSnpNoHgvs.getBatchName()))
                                                           .filter(sm -> sm.getTaxonomyAccession()
                                                                           == subSnpNoHgvs.getTaxonomyId())
                                                           .map(ProjectAccessionMapping::getEvaStudyId)
                                                           .findFirst();

        return studyId.orElse(subSnpNoHgvs.getBatchHandle() + "_" + subSnpNoHgvs.getBatchName());
    }

    private LocalDateTime getCreatedDate(SubSnpNoHgvs subSnpNoHgvs) {
        if (subSnpNoHgvs.getSsCreateTime() != null) {
            return subSnpNoHgvs.getSsCreateTime().toLocalDateTime();
        } else if (subSnpNoHgvs.getRsCreateTime() != null) {
            return subSnpNoHgvs.getRsCreateTime().toLocalDateTime();
        } else {
            return LocalDateTime.now();
        }
    }
}
