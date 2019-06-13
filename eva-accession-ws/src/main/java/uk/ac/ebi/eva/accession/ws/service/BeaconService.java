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

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.accession.core.summary.SubmittedVariantSummaryFunction;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleRequest;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleResponse;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleResponseV2;
import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleRequestBody;
import uk.ac.ebi.eva.commons.beacon.models.BeaconDataset;
import uk.ac.ebi.eva.commons.beacon.models.DatasetAlleleResponse;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@Service
public class BeaconService {

    private SubmittedVariantAccessioningService submittedVariantsService;

    private ClusteredVariantAccessioningService clusteredVariantService;

    private Function<ISubmittedVariant, String> hashingFunction =
            new SubmittedVariantSummaryFunction().andThen(new SHA1HashingFunction());

    private Function<IClusteredVariant, String> hashingFunctionClusteredVariants =
            new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

    public BeaconService(SubmittedVariantAccessioningService submittedVariantAccessioningService,
                         ClusteredVariantAccessioningService clusteredVariantAccessioningService) {
        this.submittedVariantsService = submittedVariantAccessioningService;
        this.clusteredVariantService = clusteredVariantAccessioningService;
    }

    public BeaconAlleleResponse queryBeacon(List<String> datasetStableIds, String alternateBases, String referenceBases,
                                            String chromosome, long start, String referenceGenome,
                                            boolean includeDatasetResponses) {

        BeaconAlleleResponse result = new BeaconAlleleResponse();
        BeaconAlleleRequest request = new BeaconAlleleRequest(alternateBases, referenceBases, chromosome, start,
                                                              referenceGenome, datasetStableIds,
                                                              includeDatasetResponses);
        result.setAlleleRequest(request);
        boolean exists = !getVariantByIdFields(referenceGenome, chromosome, datasetStableIds, start,
                                               referenceBases, alternateBases).isEmpty();
        result.setExists(exists);
        return result;
    }

    public List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getVariantByIdFields(
            String assembly, String contig, List<String> studies, long start, String reference, String alternate) {

        List<String> hashes = studies.stream().map(study -> hashingFunction
                .apply(new SubmittedVariant(assembly, 0, study, contig, start, reference, alternate, null)))
                                     .collect(Collectors.toList());

        List<AccessionWrapper<ISubmittedVariant, String, Long>> variants = submittedVariantsService
                .getByHashedMessageIn(hashes);

        return variants.stream()
                       .map(wrapper -> new AccessionResponseDTO<>(wrapper, SubmittedVariant::new))
                       .collect(Collectors.toList());
    }

    public BeaconAlleleResponseV2 queryBeaconClusteredVariant(String referenceGenome, String chromosome, long start,
                                                              VariantType variantType,
                                                              boolean includeDatasetResponses) {

        BeaconAlleleResponseV2 result = new BeaconAlleleResponseV2();

        BeaconAlleleRequestBody request = new BeaconAlleleRequestBody(chromosome, start, null, null, null, null, null,
                                                                      null, null, variantType.toString(),
                                                                      referenceGenome, null, null);

        result.setAlleleRequest(request);
        AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long> variant = getClusteredVariantByIdFields(
                referenceGenome, chromosome, start, variantType);

        boolean exists = variant != null;
        if (exists && includeDatasetResponses) {
            DatasetAlleleResponse datasetAlleleResponse = new DatasetAlleleResponse();
            datasetAlleleResponse.setDatasetId(variant.getAccession().toString());
            result.setDatasetAlleleResponses(Collections.singletonList(datasetAlleleResponse));
        }
        result.setExists(exists);

        return result;
    }

    public AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long> getClusteredVariantByIdFields(
            String assembly, String contig, long start, VariantType type) {

        IClusteredVariant clusteredVariant = new ClusteredVariant(assembly, 0, contig, start, type, false, null);
        String hash = hashingFunctionClusteredVariants.apply(clusteredVariant);

        List<AccessionWrapper<IClusteredVariant, String, Long>> variants = clusteredVariantService
                .getByHashedMessageIn(Collections.singletonList(hash));

        return variants.isEmpty() ? null : new AccessionResponseDTO<>(variants.get(0), ClusteredVariant::new);
    }

}
