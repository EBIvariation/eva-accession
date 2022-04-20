/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.ws.rest;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.contigalias.ContigNamingConvention;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleRequest;
import uk.ac.ebi.eva.accession.ws.dto.BeaconAlleleResponse;
import uk.ac.ebi.eva.accession.ws.dto.BeaconError;
import uk.ac.ebi.eva.accession.ws.service.SubmittedVariantsBeaconService;

import javax.servlet.http.HttpServletResponse;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/v1/submitted-variants")
@Api(tags = {"Submitted variants"})
public class SubmittedVariantsRestController {

    private SubmittedVariantsBeaconService submittedVariantsBeaconService;

    private SubmittedVariantAccessioningService service;

    public SubmittedVariantsRestController(
            SubmittedVariantAccessioningService service, SubmittedVariantsBeaconService submittedVariantsBeaconService) {
        this.service = service;
        this.submittedVariantsBeaconService = submittedVariantsBeaconService;
    }

    /**
     * In case the SS is not active and was merged into several other active SSs, an exception AccessionMergedException
     * will be thrown and a redirection to an active SS will be done by {@link EvaControllerAdvice}. Although it is
     * not entirely correct, it was decided to return only one of those merges as redirection, doesn't matter which one.
     */
    @ApiOperation(value = "Find submitted variants (SS) by identifier", notes = "This endpoint returns the submitted "
            + "variants (SS) represented by a given identifier. For a description of the response, see "
            + "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#submitted-variant-subsnp-or-ss")
    @GetMapping(value = "/{identifier}", produces = "application/json")
    public ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>> get(
            @PathVariable @ApiParam(value = "Numerical identifier of a submitted variant, e.g.: 5000000000",
                                    required = true) Long identifier,
            @RequestParam(required = false) @ApiParam(value = "Contig naming convention desired, default is INSDC")
                    ContigNamingConvention contigNamingConvention)
            throws AccessionMergedException, AccessionDoesNotExistException {
        try {
            return ResponseEntity.ok(service.getAllByAccession(identifier, contigNamingConvention).stream()
                                            .map(this::toDTO).collect(Collectors.toList()));
        } catch (NoSuchElementException e) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, e.getMessage());
        } catch (AccessionDeprecatedException e) {
            // not done with an exception handler because the only way to get the accession parameter would be parsing
            // the exception message
            return ResponseEntity.status(HttpStatus.GONE).body(getDeprecatedSubmittedVariant(identifier));
        }
    }

    private AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long> toDTO(
            AccessionWrapper<ISubmittedVariant, String, Long> submittedVariantWrapper) {
        return new AccessionResponseDTO<>(submittedVariantWrapper, SubmittedVariant::new);
    }

    /**
     * Retrieve the information in the collection for inactive objects.
     *
     * This method is necessary because the behaviour of BasicRestController is to return the HttpStatus.GONE with an
     * error message in the body. We want instead to return the HttpStatus.GONE with the variant in the body.
     */
    private List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getDeprecatedSubmittedVariant(
            Long identifier) {
        return Collections.singletonList(
                new AccessionResponseDTO<>(
                        service.getLastInactive(identifier),
                        SubmittedVariant::new));
    }

    @GetMapping(value = "/beacon/query", produces = "application/json")
    public BeaconAlleleResponse doesVariantExist(@RequestParam(name="assemblyId") String assembly,
                                                 @RequestParam(name="referenceName") String chromosome,
                                                 @RequestParam(name="datasetIds") List<String> studies,
                                                 @RequestParam(name="start") long start,
                                                 @RequestParam(name="referenceBases") String reference,
                                                 @RequestParam(name="alternateBases") String alternate,
                                                 HttpServletResponse response) {
        if (start < 1) {
            int responseStatus = HttpServletResponse.SC_BAD_REQUEST;
            response.setStatus(responseStatus);
            return getBeaconResponseObjectWithError(alternate, reference, chromosome, start, assembly, studies,
                                                    responseStatus,
                                                    "Please provide a positive number as start position");
        }
        try {
            return submittedVariantsBeaconService.queryBeacon(studies, alternate, reference, chromosome, start,
                                                              assembly, false);
        }
        catch (Exception ex) {
            int responseStatus = HttpServletResponse.SC_INTERNAL_SERVER_ERROR;
            response.setStatus(responseStatus);
            return getBeaconResponseObjectWithError(alternate, reference, chromosome, start, assembly, studies,
                                                    responseStatus, "Unexpected Error: " + ex.getMessage());
        }
    }

    private BeaconAlleleResponse getBeaconResponseObjectWithError(String alternate, String reference, String chromosome,
                                                                  long start, String assembly, List<String> studies,
                                                                  int errorCode, String errorMessage) {
        BeaconAlleleResponse result = new BeaconAlleleResponse();
        BeaconAlleleRequest request = new BeaconAlleleRequest(alternate, reference, chromosome, start,
                                                              assembly, studies, false);
        result.setAlleleRequest(request);
        result.setError(new BeaconError(errorCode, errorMessage));
        return result;
    }

    @ApiOperation(value = "Find submitted variants (SS) by the identifying fields", notes = "This endpoint returns "
            + "the submitted variants (SS) represented by a given identifier. For a description of the response, see "
            + "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#submitted-variant"
            + "-subsnp-or-ss")
    @GetMapping(produces = "application/json")
    public ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>> getByIdFields(
            @RequestParam(name="assemblyId") String assembly,
            @RequestParam(name="referenceName") String chromosome,
            @RequestParam(name="datasetIds") List<String> studies,
            @RequestParam(name="start") long start,
            @RequestParam(name="referenceBases") String reference,
            @RequestParam(name="alternateBases") String alternate) {
        try {
            return ResponseEntity.ok(
                    submittedVariantsBeaconService.getVariantByIdFields(assembly, chromosome, studies, start, reference,
                                                                        alternate));
        }
        catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(new ArrayList<>());
        }
    }
}
