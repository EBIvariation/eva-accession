/*
 * Copyright 2022 EMBL - European Bioinformatics Institute
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
 */

package uk.ac.ebi.eva.accession.core.batch.io;

import com.mongodb.ReadPreference;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class ClusteredVariantDeprecationWriter implements ItemWriter<ClusteredVariantEntity> {
    private final String assemblyAccession;
    private final MongoTemplate mongoTemplate;
    private final SubmittedVariantAccessioningService submittedVariantAccessioningService;
    private final Long accessioningMonotonicInitRs;

    /**
     * This will be suffixed to the ID field of the deprecation operation that is being written.
     * This enables efficient searching of these operations by using RS_DEPRECATED_<suffix> in a regular expression.
     * See <a href="https://www.mongodb.com/docs/v4.0/reference/operator/query/regex/#index-use">MongoDB Regex reference.</a>
     */
    private final String deprecationIdSuffix;
    private final String deprecationReason;

    public ClusteredVariantDeprecationWriter(String assemblyAccession, MongoTemplate mongoTemplate,
                                             SubmittedVariantAccessioningService submittedVariantAccessioningService,
                                             Long accessioningMonotonicInitRs,
                                             String deprecationIdSuffix, String deprecationReason) {
        ReadPreference readPreference = mongoTemplate.getMongoDbFactory().getDb().getReadPreference();
        if (!readPreference.equals(ReadPreference.primary())) {
            throw new IllegalStateException("Read preference setting should be primary to deprecate variants!");
        }
        this.assemblyAccession = assemblyAccession;
        this.mongoTemplate = mongoTemplate;
        this.submittedVariantAccessioningService = submittedVariantAccessioningService;
        this.accessioningMonotonicInitRs = accessioningMonotonicInitRs;
        this.deprecationIdSuffix = deprecationIdSuffix;
        this.deprecationReason = deprecationReason;
    }

    @Override
    public void write(List<? extends ClusteredVariantEntity> cvesToDeprecate) {
        List<? extends ClusteredVariantEntity> cvesToDeprecateInCVE = cvesToDeprecate.stream().filter(
                cve -> (cve.getAccession() >= accessioningMonotonicInitRs)).collect(Collectors.toList());
        List<? extends ClusteredVariantEntity> cvesToDeprecateInDbsnpCVE = cvesToDeprecate.stream().filter(
                cve -> (cve.getAccession() < accessioningMonotonicInitRs)).collect(Collectors.toList());
        deprecateVariants(cvesToDeprecateInCVE, ClusteredVariantEntity.class);
        deprecateVariants(cvesToDeprecateInDbsnpCVE, DbsnpClusteredVariantEntity.class);
    }

    private void deprecateVariants(List<? extends ClusteredVariantEntity> cvesToDeprecate,
                           Class<? extends ClusteredVariantEntity> cveCollectionToUse) {

        if (cvesToDeprecate.size() > 0) {
            Set<Long> rsIDsToRemove = cvesToDeprecate.stream().map(AccessionedDocument::getAccession).collect(
                    Collectors.toSet());
            Set<Long> rsIdsAssociatedWithExistingSS =
                    this.submittedVariantAccessioningService
                            .getByClusteredVariantAccessionIn(new ArrayList<>(rsIDsToRemove))
                            .stream().filter(result -> result.getData().getReferenceSequenceAccession()
                                .equals(this.assemblyAccession))
                            .map(sve -> sve.getData().getClusteredVariantAccession())
                            .collect(Collectors.toSet());
            rsIDsToRemove.removeAll(rsIdsAssociatedWithExistingSS);

            cvesToDeprecate = cvesToDeprecate.stream()
                                             .filter(cve -> rsIDsToRemove.contains(cve.getAccession()))
                                             .collect(Collectors.toList());
            Set<String> rsHashesToRemove = cvesToDeprecate.stream()
                                                          .map(ClusteredVariantEntity::getId)
                                                          .collect(Collectors.toSet());
            Class<? extends EventDocument<IClusteredVariant, Long, ? extends ClusteredVariantInactiveEntity>>
                    cvoeCollectionToUse = cveCollectionToUse.equals(ClusteredVariantEntity.class) ?
                    ClusteredVariantOperationEntity.class : DbsnpClusteredVariantOperationEntity.class;
            writeDeprecationOperation(cvesToDeprecate, cvoeCollectionToUse);
            this.mongoTemplate.findAllAndRemove(query(where("_id").in(rsHashesToRemove)), cveCollectionToUse);
        }
    }

    private void writeDeprecationOperation(List<? extends ClusteredVariantEntity> cvesToDeprecate,
                                   Class<? extends EventDocument<IClusteredVariant, Long,
                                           ? extends ClusteredVariantInactiveEntity>> cvoeCollectionToUse) {
        List<ClusteredVariantOperationEntity> cvoesToWrite = cvesToDeprecate.stream().map(cve -> {
            ClusteredVariantOperationEntity cvoe = new ClusteredVariantOperationEntity();
            cvoe.fill(EventType.DEPRECATED, cve.getAccession(), null,
                      this.deprecationReason,
                      Collections.singletonList(new ClusteredVariantInactiveEntity(cve)));
            cvoe.setId(String.join("_", Arrays.asList("RS_DEPRECATED", this.deprecationIdSuffix, cve.getId())));
            return cvoe;
        }).collect(Collectors.toList());
        Set<String> operationIdsToWrite = cvoesToWrite.stream().map(EventDocument::getId).collect(Collectors.toSet());
        Set<String> alreadyExistingIds = this.mongoTemplate.find(query(where("_id").in(operationIdsToWrite)),
                                                                 cvoeCollectionToUse).stream().map(EventDocument::getId)
                                                           .collect(Collectors.toSet());
        operationIdsToWrite.removeAll(alreadyExistingIds);
        cvoesToWrite = cvoesToWrite.stream().filter(svoe -> operationIdsToWrite.contains(svoe.getId())).collect(
                Collectors.toList());
        this.mongoTemplate.insert(cvoesToWrite, cvoeCollectionToUse);
    }
}
