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
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.IEvent;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.IAccessionedObject;
import uk.ac.ebi.ampt2d.commons.accession.rest.controllers.BasicRestController;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;

import java.util.Collections;
import java.util.List;

@RestController
@RequestMapping(value = "/v1/submitted-variants")
@Api(tags = {"Submitted variants"})
public class SubmittedVariantsRestController {

    private final BasicRestController<SubmittedVariant, ISubmittedVariant, String, Long> basicRestController;

    private SubmittedVariantAccessioningService service;

    public SubmittedVariantsRestController(
            BasicRestController<SubmittedVariant, ISubmittedVariant, String, Long> basicRestController,
            SubmittedVariantAccessioningService service) {
        this.basicRestController = basicRestController;
        this.service = service;
    }

    @ApiOperation(value = "Find submitted variants (SS) by identifier", notes = "This endpoint returns the submitted "
            + "variants (SS) represented by a given identifier. For a description of the response, see "
            + "https://github.com/EBIvariation/eva-accession/wiki/Import-accessions-from-dbSNP#submitted-variant-subsnp-or-ss")
    @GetMapping(value = "/{identifier}", produces = "application/json")
    public ResponseEntity<List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>>> get(
            @PathVariable @ApiParam(value = "Numerical identifier of a submitted variant, e.g.: 5000000000",
                                    required = true) Long identifier)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        try {
            return ResponseEntity.ok(Collections.singletonList(basicRestController.get(identifier)));
        } catch (AccessionDeprecatedException e) {
            // not done with an exception handler because the only way to get the accession parameter would be parsing
            // the exception message
            return ResponseEntity.status(HttpStatus.GONE).body(getDeprecatedSubmittedVariant(identifier));
        }
    }

    /**
     * Retrieve the information in the collection for inactive objects.
     *
     * This method is necessary because the behaviour of BasicRestController is to return the HttpStatus.GONE with an
     * error exception message in the body. We want instead to return the HttpStatus.GONE with the variant in the body
     */
    private List<AccessionResponseDTO<SubmittedVariant, ISubmittedVariant, String, Long>> getDeprecatedSubmittedVariant(
            Long identifier) {
        IAccessionedObject<ISubmittedVariant, ?, Long> lastInactiveWithoutType = service.getLastInactive(identifier);
        IAccessionedObject<ISubmittedVariant, String, Long> lastInactive =
                (IAccessionedObject<ISubmittedVariant, String, Long>) lastInactiveWithoutType;

        return Collections.singletonList(
                new AccessionResponseDTO<>(
                        new AccessionWrapper<>(identifier,
                                               lastInactive.getHashedMessage(),
                                               lastInactive.getModel()),
                        SubmittedVariant::new));
    }
}

