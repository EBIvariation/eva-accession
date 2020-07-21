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

import org.apache.commons.collections.CollectionUtils;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleRequest;
import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleResponse;
import uk.ac.ebi.eva.commons.beacon.models.BeaconDatasetAlleleResponse;
import uk.ac.ebi.eva.commons.beacon.models.BeaconError;
import uk.ac.ebi.eva.commons.beacon.models.Chromosome;
import uk.ac.ebi.eva.commons.beacon.models.KeyValuePair;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Service
public class ClusteredVariantsBeaconService {

    private static final String BEACON_ID = "ebi-eva-beacon";

    private static final String API_VERSION = "";

    private ClusteredVariantAccessioningService clusteredVariantService;

    private HumanDbsnpClusteredVariantAccessioningService humanService;

    private SubmittedVariantAccessioningService submittedVariantsService;

    public ClusteredVariantsBeaconService(
            @Qualifier("nonhumanActiveService") ClusteredVariantAccessioningService clusteredVariantAccessioningService,
            @Qualifier("humanService") HumanDbsnpClusteredVariantAccessioningService humanService,
            SubmittedVariantAccessioningService submittedVariantsService) {
        this.clusteredVariantService = clusteredVariantAccessioningService;
        this.humanService = humanService;
        this.submittedVariantsService = submittedVariantsService;
    }

    public BeaconAlleleResponse queryBeaconClusteredVariant(String referenceGenome, String chromosome,
                                                            long start, VariantType variantType,
                                                            boolean includeDatasetResponses) {

        BeaconAlleleResponse beaconAlleleResponseNonHuman = queryBeaconClusteredVariantNonHuman(
                referenceGenome, chromosome, start, variantType, includeDatasetResponses);

        BeaconAlleleResponse beaconAlleleResponseHuman = queryBeaconClusteredVariantHuman(
                referenceGenome, chromosome, start, variantType, includeDatasetResponses);

        return mergeResponses(beaconAlleleResponseNonHuman, beaconAlleleResponseHuman);
    }

    private BeaconAlleleResponse queryBeaconClusteredVariantNonHuman(String referenceGenome, String chromosome,
                                                                     long start, VariantType variantType,
                                                                     boolean includeDatasetResponses) {
        List<AccessionWrapper<IClusteredVariant, String, Long>> variants =
                clusteredVariantService.getByIdFields(referenceGenome, chromosome, start, variantType);
        List<BeaconDatasetAlleleResponse> datasetAlleleResponses = new ArrayList<>();
        boolean isVariantExists = !variants.isEmpty();
        if (isVariantExists && includeDatasetResponses) {
            List<Long> identifiers = variants.stream().map(this::toDTO).map(AccessionResponseDTO::getAccession)
                    .collect(Collectors.toList());
            datasetAlleleResponses = getBeaconDatasetAlleleResponses(identifiers);
        }

        return buildResponse(referenceGenome, chromosome, start, variantType, isVariantExists, datasetAlleleResponses);
    }

    private AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long> toDTO(
            AccessionWrapper<IClusteredVariant, String, Long> clusteredVariantWrapper) {
        return new AccessionResponseDTO<>(clusteredVariantWrapper, ClusteredVariant::new);
    }

    private List<BeaconDatasetAlleleResponse> getBeaconDatasetAlleleResponses(List<Long> clusteredVariantAccession) {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> submittedVariants =
                submittedVariantsService.getByClusteredVariantAccessionIn(clusteredVariantAccession);

        Map<String, Set<String>> projects = new HashMap<>();
        submittedVariants.forEach(variant -> {
            String projectAccession = variant.getData().getProjectAccession();
            String submittedVariantAccession = "ss" + variant.getAccession().toString();
            projects.computeIfAbsent(projectAccession, key -> new HashSet<>()).add(submittedVariantAccession);
        });

        List<BeaconDatasetAlleleResponse> datasetAlleleResponses = new ArrayList<>();
        projects.forEach((project, ids) -> {
            BeaconDatasetAlleleResponse datasetAlleleResponse = new BeaconDatasetAlleleResponse();
            datasetAlleleResponse.setDatasetId(project);

            String rsValue = clusteredVariantAccession.stream().map(Object::toString)
                    .collect(Collectors.joining(",", "rs", ""));
            KeyValuePair rs = new KeyValuePair().key("RS ID").value(rsValue);
            KeyValuePair ss = new KeyValuePair().key("SS IDs").value(String.join(",", ids));
            List<KeyValuePair> info = new ArrayList<>(Arrays.asList(rs, ss));
            datasetAlleleResponse.setInfo(info);

            datasetAlleleResponse.exists(true);
            datasetAlleleResponses.add(datasetAlleleResponse);
        });
        return datasetAlleleResponses;
    }

    private BeaconAlleleResponse buildResponse(String referenceGenome, String chromosome, long start,
                                               VariantType variantType, boolean exists,
                                               List<BeaconDatasetAlleleResponse> datasetAlleleResponses) {
        BeaconAlleleResponse response = new BeaconAlleleResponse();

        BeaconAlleleRequest request = new BeaconAlleleRequest();
        request.setReferenceName(Chromosome.fromValue(chromosome));
        request.setStart(start);
        request.setVariantType(variantType.toString());
        request.setAssemblyId(referenceGenome);

        response.setAlleleRequest(request);

        if (!datasetAlleleResponses.isEmpty()) {
            response.setDatasetAlleleResponses(datasetAlleleResponses);
        }

        response.setExists(exists);
        return response;
    }

    private BeaconAlleleResponse queryBeaconClusteredVariantHuman(String referenceGenome, String chromosome,
                                                                  long start, VariantType variantType,
                                                                  boolean includeDatasetResponses) {
        List<AccessionWrapper<IClusteredVariant, String, Long>> variant =
                humanService.getByIdFields(referenceGenome, chromosome, start, variantType);
        List<BeaconDatasetAlleleResponse> getBeaconDatasetAlleleResponsesHuman = new ArrayList<>();
        if (!variant.isEmpty() && includeDatasetResponses) {
            String identifier = variant.get(0).getAccession().toString();
            getBeaconDatasetAlleleResponsesHuman = getBeaconDatasetAlleleResponsesHuman(identifier);
        }
        return buildResponse(referenceGenome, chromosome, start, variantType, !variant.isEmpty(),
                             getBeaconDatasetAlleleResponsesHuman);
    }

    private List<BeaconDatasetAlleleResponse> getBeaconDatasetAlleleResponsesHuman(String clusteredVariantAccession) {
        List<BeaconDatasetAlleleResponse> datasetAlleleResponses = new ArrayList<>();

        BeaconDatasetAlleleResponse datasetAlleleResponse = new BeaconDatasetAlleleResponse();
        KeyValuePair rs = new KeyValuePair().key("RS ID").value("rs" + clusteredVariantAccession);
        List<KeyValuePair> info = new ArrayList<>(Collections.singletonList(rs));
        datasetAlleleResponse.setInfo(info);
        datasetAlleleResponse.exists(true);
        datasetAlleleResponses.add(datasetAlleleResponse);

        return datasetAlleleResponses;
    }

    private BeaconAlleleResponse mergeResponses(BeaconAlleleResponse nonHumanResponse,
                                                BeaconAlleleResponse humanResponse) {
        BeaconAlleleResponse response = new BeaconAlleleResponse();
        response.beaconId(BEACON_ID);
        response.apiVersion(API_VERSION);
        response.setAlleleRequest(nonHumanResponse.getAlleleRequest());
        response.exists(nonHumanResponse.isExists() || humanResponse.isExists());

        if (response.isExists()) {
            List<BeaconDatasetAlleleResponse> datasetAlleleResponses = new ArrayList<>();
            if (!CollectionUtils.isEmpty(nonHumanResponse.getDatasetAlleleResponses())) {
                datasetAlleleResponses.addAll(nonHumanResponse.getDatasetAlleleResponses());
            }
            if (!CollectionUtils.isEmpty(humanResponse.getDatasetAlleleResponses())) {
                datasetAlleleResponses.addAll(humanResponse.getDatasetAlleleResponses());
            }
            if (!CollectionUtils.isEmpty(datasetAlleleResponses)) {
                response.setDatasetAlleleResponses(datasetAlleleResponses);
            }
        }

        return response;
    }

    public BeaconAlleleResponse getBeaconResponseObjectWithError(String reference, long start, String assembly,
                                                                  VariantType variantType, int errorCode,
                                                                  String errorMessage) {
        BeaconAlleleRequest request = new BeaconAlleleRequest();
        request.setReferenceName(Chromosome.fromValue(reference));
        request.setStart(start);
        request.setVariantType(variantType.toString());
        request.setAssemblyId(assembly);

        BeaconError error = new BeaconError();
        error.setErrorCode(errorCode);
        error.setErrorMessage(errorMessage);

        BeaconAlleleResponse response = new BeaconAlleleResponse();
        response.setAlleleRequest(request);
        response.setError(error);
        return response;
    }
}
