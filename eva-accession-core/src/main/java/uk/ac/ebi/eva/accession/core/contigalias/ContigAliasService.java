/*
 *
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
 *
 */
package uk.ac.ebi.eva.accession.core.contigalias;

import org.springframework.web.client.RestTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.IAccessionedObject;

import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigAliasResponse;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigAliasTranslator;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;

public class ContigAliasService {

    public static final String CONTIG_ALIAS_CHROMOSOMES_GENBANK_ENDPOINT = "/v1/chromosomes/genbank/";

    private final RestTemplate restTemplate;

    private final String contigAliasUrl;

    public ContigAliasService(RestTemplate restTemplate, String contigAliasUrl) {
        this.restTemplate = restTemplate;
        this.contigAliasUrl = contigAliasUrl;
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getSubmittedVariantsWithTranslatedContig(
            List<AccessionWrapper<ISubmittedVariant, String, Long>> submittedVariants,
            ContigNamingConvention contigNamingConvention) throws NoSuchElementException {
        if (skipContigTranslation(contigNamingConvention)) return submittedVariants;
        List<AccessionWrapper<ISubmittedVariant, String, Long>> submittedVariantsAfterContigAlias = new ArrayList<>();
        for (AccessionWrapper<ISubmittedVariant, String, Long> submittedVariant : submittedVariants) {
            String translatedContig = translateContig(submittedVariant.getData().getContig(), contigNamingConvention);
            submittedVariantsAfterContigAlias.add(
                    createSubmittedVariantAccessionWrapperWithNewContig(submittedVariant, translatedContig));
        }
        return submittedVariantsAfterContigAlias;
    }

    /**
     * Contigs are stored in INSDC naming convention in the accessioning database (default convention).
     *
     * When no naming convention is specified (naming convention is null) or naming convention is NO_REPLACEMENT or
     * naming convention is INSDC there is no need for contig translation so it can be skipped.
     */
    private boolean skipContigTranslation(ContigNamingConvention contigNamingConvention) {
        //Contigs are stored in INSDC naming convention in the accessioning database so no need for translation
        return contigNamingConvention == null ||
                contigNamingConvention.equals(ContigNamingConvention.INSDC) ||
                contigNamingConvention.equals(ContigNamingConvention.NO_REPLACEMENT);
    }

    /**
     * Query contig alias service to translate the contig to the desired naming convention
     */
    private String translateContig(String genbankContig, ContigNamingConvention contigNamingConvention) {
        String url = contigAliasUrl + CONTIG_ALIAS_CHROMOSOMES_GENBANK_ENDPOINT + genbankContig;
        ContigAliasResponse contigAliasResponse = restTemplate.getForObject(url, ContigAliasResponse.class);
        if (contigAliasResponse == null || contigAliasResponse.getEmbedded() == null) {
            throw new NoSuchElementException("No data returned for " + url + " from the contig alias service");
        }
        return ContigAliasTranslator.getTranslatedContig(contigAliasResponse, contigNamingConvention);
    }

    private AccessionWrapper<ISubmittedVariant, String, Long> createSubmittedVariantAccessionWrapperWithNewContig(
            AccessionWrapper<ISubmittedVariant, String, Long> accessionWrapper, String newContig) {
        ISubmittedVariant data = accessionWrapper.getData();
        ISubmittedVariant dataAfterContigAlias = new SubmittedVariant(data.getReferenceSequenceAccession(),
                                                                      data.getTaxonomyAccession(),
                                                                      data.getProjectAccession(),
                                                                      newContig,
                                                                      data.getStart(),
                                                                      data.getReferenceAllele(),
                                                                      data.getAlternateAllele(),
                                                                      data.getClusteredVariantAccession(),
                                                                      data.isSupportedByEvidence(),
                                                                      data.isAssemblyMatch(),
                                                                      data.isAllelesMatch(),
                                                                      data.isValidated(),
                                                                      data.getCreatedDate());
        return new AccessionWrapper<>(accessionWrapper.getAccession(), accessionWrapper.getHash(), dataAfterContigAlias);
}

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getClusteredVariantsWithTranslatedContig(
        List<AccessionWrapper<IClusteredVariant, String, Long>> clusteredVariants, ContigNamingConvention contigNamingConvention) {
        if (skipContigTranslation(contigNamingConvention)) return clusteredVariants;
        List<AccessionWrapper<IClusteredVariant, String, Long>> clusteredVariantsAfterContigAlias = new ArrayList<>();
        for (AccessionWrapper<IClusteredVariant, String, Long> clusteredVariant : clusteredVariants) {
            String translatedContig = translateContig(clusteredVariant.getData().getContig(), contigNamingConvention);
            clusteredVariantsAfterContigAlias.add(
                    createClusteredVariantAccessionWrapperWithNewContig(clusteredVariant, translatedContig));
        }
        return clusteredVariantsAfterContigAlias;
    }

    private AccessionWrapper<IClusteredVariant, String, Long> createClusteredVariantAccessionWrapperWithNewContig(
            AccessionWrapper<IClusteredVariant, String, Long> accessionWrapper, String newContig) {
        IClusteredVariant data = accessionWrapper.getData();
        IClusteredVariant dataAfterContigAlias = new ClusteredVariant(data.getAssemblyAccession(),
                                                                      data.getTaxonomyAccession(),
                                                                      newContig,
                                                                      data.getStart(),
                                                                      data.getType(),
                                                                      data.isValidated(),
                                                                      data.getCreatedDate());
        return new AccessionWrapper<>(accessionWrapper.getAccession(), accessionWrapper.getHash(), dataAfterContigAlias);
    }

    public List<? extends IEvent<IClusteredVariant, Long>> getEventsWithTranslatedContig(
            List<? extends IEvent<IClusteredVariant, Long>> events, ContigNamingConvention contigNamingConvention) {
        if (skipContigTranslation(contigNamingConvention)) return events;
        List<ClusteredVariantOperationEntity> allEventsAfterContigAlias = new ArrayList<>();
        for (IEvent<? extends IClusteredVariant, Long> event : events) {
            List<? extends IAccessionedObject<? extends IClusteredVariant, ?, Long>> inactiveObjects =
                    event.getInactiveObjects();
            List<ClusteredVariantInactiveEntity> inactiveObjectsAfterContigAlias = new ArrayList<>();
            for (IAccessionedObject<? extends IClusteredVariant, ?, Long> inactiveObject : inactiveObjects) {
                IClusteredVariant clusteredVariant = inactiveObject.getModel();
                String translatedContig = translateContig(clusteredVariant.getContig(), contigNamingConvention);
                inactiveObjectsAfterContigAlias.add(createClusteredVariantInactiveEntityWithNewContig(
                        inactiveObject, clusteredVariant, translatedContig));
            }
            ClusteredVariantOperationEntity clusteredVariantOperationEntity = new ClusteredVariantOperationEntity();
            clusteredVariantOperationEntity.fill(event.getEventType(), event.getAccession(),
                                                 event.getDestinationAccession(), event.getReason(),
                                                 inactiveObjectsAfterContigAlias);
            clusteredVariantOperationEntity.setCreatedDate(event.getCreatedDate());
            allEventsAfterContigAlias.add(clusteredVariantOperationEntity);
        }
        return allEventsAfterContigAlias;
    }

    private ClusteredVariantInactiveEntity createClusteredVariantInactiveEntityWithNewContig(
            IAccessionedObject<? extends IClusteredVariant, ?, Long> inactiveObject, IClusteredVariant clusteredVariant,
            String translatedContig) {
        ClusteredVariantEntity clusteredVariantEntity = new ClusteredVariantEntity(
                inactiveObject.getAccession(),
                (String) inactiveObject.getHashedMessage(),
                clusteredVariant.getAssemblyAccession(),
                clusteredVariant.getTaxonomyAccession(),
                translatedContig,
                clusteredVariant.getStart(),
                clusteredVariant.getType(),
                clusteredVariant.isValidated(),
                clusteredVariant.getCreatedDate(),
                inactiveObject.getVersion(),
                clusteredVariant.getMapWeight());
        ClusteredVariantInactiveEntity clusteredVariantInactiveEntity =
                new ClusteredVariantInactiveEntity(clusteredVariantEntity);
        return clusteredVariantInactiveEntity;
    }
}
