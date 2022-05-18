/*
 *
 * Copyright 2019 EMBL - European Bioinformatics Institute
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

package uk.ac.ebi.eva.accession.ws.service;

import org.springframework.stereotype.Service;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleRequest;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleResponse;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class SubmittedVariantsBeaconService {

    private SubmittedVariantAccessioningService submittedVariantsService;

    public SubmittedVariantsBeaconService(SubmittedVariantAccessioningService submittedVariantAccessioningService) {
        this.submittedVariantsService = submittedVariantAccessioningService;
    }

    public BeaconAlleleResponse queryBeacon(List<String> datasetStableIds, String alternateBases, String referenceBases,
                                            String chromosome, long start, String referenceGenome,
                                            ContigNamingConvention contigNamingConvention,
                                            boolean includeDatasetResponses) {

        BeaconAlleleResponse result = new BeaconAlleleResponse();
        BeaconAlleleRequest request = new BeaconAlleleRequest(alternateBases, referenceBases, chromosome, start,
                                                              referenceGenome, datasetStableIds,
                                                              includeDatasetResponses);
        result.setAlleleRequest(request);
        boolean exists = !getVariantByIdFields(referenceGenome, chromosome, datasetStableIds, start,
                                               referenceBases, alternateBases, contigNamingConvention).isEmpty();
        result.setExists(exists);
        return result;
    }

    public List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantByIdFields(
            String assembly, String contig, List<String> studies, long start, String reference, String alternate,
            ContigNamingConvention contigNamingConvention) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> variants = submittedVariantsService
                .getAllByIdFields(assembly, contig, studies, start, reference, alternate, contigNamingConvention);
        return variants.stream()
                       .map(wrapper -> new AccessionResponseDTO<>(wrapper, SubmittedVariant::new))
                       .collect(Collectors.toList());
    }

}
