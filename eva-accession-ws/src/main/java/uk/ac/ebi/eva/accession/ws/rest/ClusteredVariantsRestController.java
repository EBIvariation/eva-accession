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
import io.swagger.annotations.Example;
import io.swagger.annotations.ExampleProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.IAccessionedObject;
import uk.ac.ebi.ampt2d.commons.accession.persistence.services.InactiveAccessionService;
import uk.ac.ebi.ampt2d.commons.accession.rest.controllers.BasicRestController;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.DbsnpClusteredVariantInactiveService;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

@RestController
@RequestMapping(value = "/v1/clustered-variants")
@Api(tags = {"Clustered variants"})
public class ClusteredVariantsRestController {

    private static final Logger logger = LoggerFactory.getLogger(ClusteredVariantsRestController.class);

    private final BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicRestController;

    private SubmittedVariantAccessioningService submittedVariantsService;

    // TODO use an inactive service that delegates in both EVA and dbSNP. only the dbSNP one exists at the moment.
    private DbsnpClusteredVariantInactiveService inactiveService;

    public ClusteredVariantsRestController(
            BasicRestController<ClusteredVariant, IClusteredVariant, String, Long> basicRestController,
            SubmittedVariantAccessioningService submittedVariantsService,
            DbsnpClusteredVariantInactiveService inactiveService) {
        this.basicRestController = basicRestController;
        this.submittedVariantsService = submittedVariantsService;
        this.inactiveService = inactiveService;
    }

    /**
     * In case the RS is not active and was merged into several other active RSs, an exception AccessionMergedException
     * will be thrown and a redirection to an active RS will be done by {@link EvaControllerAdvice}. Although it is
     * not entirely correct, it was decided to return only one of those merges as redirection, doesn't matter which one.
     */
    @ApiOperation(value = "Find clustered variants (RS) by identifier", notes = "This endpoint returns the clustered "
            + "variants (RS) represented by the given identifier. For a description of the response, see "
            + "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#clustered-variant-refsnp-or-rs")
    @GetMapping(value = "/{identifier}", produces = "application/json")
    public ResponseEntity<List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>>> get(
            @PathVariable @ApiParam(value = "Numerical identifier of a clustered variant, e.g.: 3000000000",
                                    required = true) Long identifier)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        try {
            return ResponseEntity.ok(Collections.singletonList(basicRestController.get(identifier)));
        } catch (AccessionDeprecatedException e) {
            // not done with an exception handler because the only way to get the accession parameter would be parsing
            // the exception message
            return ResponseEntity.status(HttpStatus.GONE).body(getDeprecatedClusteredVariant(identifier));
        }
    }

    /**
     * Retrieve the information in the collection for inactive objects.
     *
     * This method is necessary because the behaviour of BasicRestController is to return the HttpStatus.GONE with an
     * error exception message in the body. We want instead to return the HttpStatus.GONE with the variant in the body
     */
    private List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getDeprecatedClusteredVariant(
            Long identifier) {
        IAccessionedObject<IClusteredVariant, ?, Long> accessionedObjectWrongType = inactiveService
                .getLastEvent(identifier).getInactiveObjects().get(0);

        IAccessionedObject<IClusteredVariant, String, Long> accessionedObject =
                (IAccessionedObject<IClusteredVariant, String, Long>) accessionedObjectWrongType;

        return Collections.singletonList(
                new AccessionResponseDTO<>(
                        new AccessionWrapper<>(identifier,
                                               accessionedObject.getHashedMessage(),
                                               accessionedObject.getModel()),
                        ClusteredVariant::new));
    }

    @ApiOperation(value = "Find submitted variants (SS) by clustered variant identifier (RS)", notes = "Given a "
            + "clustered variant identifier (RS), this endpoint returns all the submitted variants (SS) linked to"
            + " the former. For a description of the response, see "
            + "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#submitted-variant-subsnp-or-ss")
    @GetMapping(value = "/{identifier}/submitted", produces = "application/json")
    public List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getSubmittedVariants(
            @PathVariable @ApiParam(value = "Numerical identifier of a clustered variant, e.g.: 869808637",
                    required = true) Long identifier)
            throws AccessionDoesNotExistException, AccessionDeprecatedException, AccessionMergedException {
        // trigger the checks. if the identifier was merged, the EvaControllerAdvice will redirect to the correct URL
        basicRestController.get(identifier);

        List<AccessionWrapper<ISubmittedVariant, String, Long>> submittedVariants =
                submittedVariantsService.getByClusteredVariantAccessionIn(Collections.singletonList(identifier));

        return submittedVariants.stream()
                                .map(wrapper -> new AccessionResponseDTO<>(wrapper, SubmittedVariant::new))
                                .collect(Collectors.toList());
    }
}

