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
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;
import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleRequest;
import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleResponse;
import uk.ac.ebi.eva.commons.beacon.models.BeaconDatasetAlleleResponse;
import uk.ac.ebi.eva.commons.beacon.models.Chromosome;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

@Service
public class ClusteredVariantsBeaconService {

    private ClusteredVariantAccessioningService clusteredVariantService;

    private Function<IClusteredVariant, String> hashingFunctionClusteredVariants =
            new ClusteredVariantSummaryFunction().andThen(new SHA1HashingFunction());

    public ClusteredVariantsBeaconService(ClusteredVariantAccessioningService clusteredVariantAccessioningService) {
        this.clusteredVariantService = clusteredVariantAccessioningService;
    }

    public BeaconAlleleResponse queryBeaconClusteredVariant(String referenceGenome, String chromosome, long start,
                                                            VariantType variantType, boolean includeDatasetResponses) {

        BeaconAlleleResponse result = new BeaconAlleleResponse();

        BeaconAlleleRequest request = new BeaconAlleleRequest();
        request.setReferenceName(Chromosome.fromValue(chromosome));
        request.setStart(start);
        request.setVariantType(variantType.toString());
        request.setAssemblyId(referenceGenome);

        result.setAlleleRequest(request);
        AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long> variant = getClusteredVariantByIdFields(
                referenceGenome, chromosome, start, variantType);

        boolean exists = variant != null;
        if (exists && includeDatasetResponses) {
            BeaconDatasetAlleleResponse datasetAlleleResponse = new BeaconDatasetAlleleResponse();
            datasetAlleleResponse.setDatasetId("rs" + variant.getAccession().toString());
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
