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
package uk.ac.ebi.eva.accession.core.repository.nonhuman.eva;

import org.springframework.stereotype.Repository;
import uk.ac.ebi.ampt2d.commons.accession.persistence.models.AccessionProjection;
import uk.ac.ebi.ampt2d.commons.accession.persistence.repositories.IAccessionedObjectRepository;

import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;

import java.util.List;

@Repository
public interface SubmittedVariantAccessioningRepository extends
        IAccessionedObjectRepository<SubmittedVariantEntity, Long> {

    List<SubmittedVariantEntity> findByClusteredVariantAccessionIn(List<Long> clusteredVariantAccession);

    List<AccessionProjection<Long>> findByAccessionGreaterThanEqualAndAccessionLessThanEqual(Long start, Long end);
}
