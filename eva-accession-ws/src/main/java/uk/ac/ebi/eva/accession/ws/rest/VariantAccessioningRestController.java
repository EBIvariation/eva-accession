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

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import uk.ac.ebi.eva.accession.core.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.SubmittedVariantModel;
import uk.ac.ebi.ampt2d.commons.accession.rest.BasicRestController;

@RestController
@RequestMapping(value = "/v1/variant")
public class VariantAccessioningRestController extends BasicRestController<SubmittedVariantModel, SubmittedVariantDTO, Long> {

    public VariantAccessioningRestController(SubmittedVariantAccessioningService service) {
        super(service, model -> new SubmittedVariantDTO(model.getAssemblyAccession(),
                                                        model.getTaxonomyAccession(),
                                                        model.getProjectAccession(),
                                                        model.getContig(),
                                                        model.getStart(),
                                                        model.getReferenceAllele(),
                                                        model.getAlternateAllele(),
                                                        model.isSupportedByEvidence()));
    }

}

