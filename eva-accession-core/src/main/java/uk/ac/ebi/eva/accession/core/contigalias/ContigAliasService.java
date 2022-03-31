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

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;

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
            List<AccessionWrapper<ISubmittedVariant, String, Long>> allByAccession, ContigAliasNaming contigAliasNaming)
            throws NoSuchElementException {
        if (contigAliasNaming == null || contigAliasNaming.equals(ContigAliasNaming.INSDC) ||
                contigAliasNaming.equals(ContigAliasNaming.NO_REPLACEMENT)) {
            //Contigs are stored in INSDC naming convention in the accessioning database so no need for translation
            return allByAccession;
        }

        List<AccessionWrapper<ISubmittedVariant, String, Long>> allByAccessionAfterContigAlias = new ArrayList<>();
        for (AccessionWrapper<ISubmittedVariant, String, Long> submittedVariant : allByAccession) {
            String genbankContig = submittedVariant.getData().getContig();
            String url = contigAliasUrl + CONTIG_ALIAS_CHROMOSOMES_GENBANK_ENDPOINT + genbankContig;
            ContigAliasResponse contigAliasResponse = restTemplate.getForObject(url, ContigAliasResponse.class);
            if (contigAliasResponse == null || contigAliasResponse.getEmbedded() == null) {
                throw new NoSuchElementException("Not data returned for " + url + " from the contig alias service");
            }
            String translatedContig = getTranslatedContig(contigAliasResponse, contigAliasNaming);
            allByAccessionAfterContigAlias.add(createAccessionWrapperWithNewContig(submittedVariant, translatedContig));
        }
        return allByAccessionAfterContigAlias;
    }

    private AccessionWrapper<ISubmittedVariant, String, Long> createAccessionWrapperWithNewContig(
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

    private String getTranslatedContig(ContigAliasResponse contigAliasResponse, ContigAliasNaming contigAliasNaming) {
        ContigAliasChromosome contigAliasChromosome = contigAliasResponse.getEmbedded().getContigAliasChromosomes().get(0);
        String contig;
        switch (contigAliasNaming) {
            case GENBANK_SEQUENCE_NAME:
                contig = contigAliasChromosome.getName();
                break;
            case REFSEQ:
                contig = contigAliasChromosome.getRefseq();
                break;
            case UCSC:
                contig = contigAliasChromosome.getUcscName();
                break;
            case ENA_SEQUENCE_NAME:
                contig = contigAliasChromosome.getEnaSequenceName();
                break;
            case MD5_CHECKSUM:
                contig = contigAliasChromosome.getMd5checksum();
                break;
            case TRUNC512_CHECKSUM:
                contig = contigAliasChromosome.getTrunc512checksum();
                break;
            default:
                contig = contigAliasChromosome.getGenbank();
        }

       if (contig != null) {
           return contig;
       } else {
           throw new NoSuchElementException("Contig " + contigAliasChromosome.getGenbank() +
                                                    " could not be translated to " + contigAliasNaming);
       }
    }
}
