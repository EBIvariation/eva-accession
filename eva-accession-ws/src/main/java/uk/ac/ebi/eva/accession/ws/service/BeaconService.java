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
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleRequest;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleResponse;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class BeaconService {

    private SubmittedVariantAccessioningService submittedVariantsService;

    private Function<ISubmittedVariant, String> hashingFunction =
            new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());

    public BeaconService(SubmittedVariantAccessioningService submittedVariantAccessioningService) {
        this.submittedVariantsService = submittedVariantAccessioningService;
    }

    public BeaconAlleleResponse queryBeacon(List<String> datasetStableIds, String alternateBases, String referenceBases,
                                            String chromosome, long start, String referenceGenome,
                                            boolean includeDatasetResponses) {

        BeaconAlleleResponse result = new BeaconAlleleResponse();
        BeaconAlleleRequest request = new BeaconAlleleRequest(alternateBases, referenceBases, chromosome, start,
                                                              referenceGenome, datasetStableIds,
                                                              includeDatasetResponses);
        result.setAlleleRequest(request);
        boolean exists = !getVariantByIdFields(referenceGenome, chromosome, datasetStableIds.get(0), start,
                                               referenceBases, alternateBases).isEmpty();
        result.setExists(exists);
        return result;
    }

    public List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantByIdFields(
            String assembly, String contig, String study, long start, String reference, String alternate) {

        ISubmittedVariant variant = new SubmittedVariant(assembly, 0, study, contig, start, reference, alternate, null);
        String hash = hashingFunction.apply(variant);

        List<AccessionWrapper<ISubmittedVariant, String, Long>> variants = submittedVariantsService
                .getByHashedMessageIn(
                        Collections.singletonList(hash));

        return variants.stream()
                       .map(wrapper -> new AccessionResponseDTO<>(wrapper, SubmittedVariant::new))
                       .collect(Collectors.toList());
    }

}
