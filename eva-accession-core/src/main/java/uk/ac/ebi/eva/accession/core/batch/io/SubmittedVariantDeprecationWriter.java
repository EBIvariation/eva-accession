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
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.springframework.batch.item.ItemWriter;
import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.AccessionedDocument;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpSubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.model.eva.ClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantInactiveEntity;
import uk.ac.ebi.eva.accession.core.model.eva.SubmittedVariantOperationEntity;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.groovy.commons.EVAObjectModelUtils;

import java.util.*;
import java.util.stream.Collectors;

import static org.springframework.data.mongodb.core.query.Criteria.where;
import static org.springframework.data.mongodb.core.query.Query.query;

public class SubmittedVariantDeprecationWriter implements ItemWriter<SubmittedVariantEntity> {

    private final String assemblyAccession;
    private final MongoTemplate mongoTemplate;
    private final ClusteredVariantAccessioningService clusteredVariantAccessioningService;
    private final Long accessioningMonotonicInitSs;
    private final ClusteredVariantDeprecationWriter clusteredVariantDeprecationWriter;

    /**
     * This will be suffixed to the ID field of the deprecation operation that is being written.
     * This enables efficient searching of these operations by using SS_DEPRECATED_<suffix> in a regular expression.
     * See <a href="https://www.mongodb.com/docs/v4.0/reference/operator/query/regex/#index-use">MongoDB Regex reference.</a>
     */
    private final String deprecationIdSuffix;
    private final String deprecationReason;

    private int numDeprecatedSubmittedEntities;

    public SubmittedVariantDeprecationWriter(String assemblyAccession, MongoTemplate mongoTemplate,
                                             SubmittedVariantAccessioningService submittedVariantAccessioningService,
                                             ClusteredVariantAccessioningService clusteredVariantAccessioningService,
                                             Long accessioningMonotonicInitSs, Long accessioningMonotonicInitRs,
                                             String deprecationIdSuffix, String deprecationReason) {
        ReadPreference readPreference = mongoTemplate.getMongoDbFactory().getDb().getReadPreference();
        if (!readPreference.equals(ReadPreference.primary())) {
            throw new IllegalStateException("Read preference setting should be primary to deprecate variants!");
        }
        this.assemblyAccession = assemblyAccession;
        this.mongoTemplate = mongoTemplate;
        this.clusteredVariantAccessioningService = clusteredVariantAccessioningService;
        this.accessioningMonotonicInitSs = accessioningMonotonicInitSs;
        this.deprecationIdSuffix = deprecationIdSuffix;
        this.deprecationReason = deprecationReason;
        this.clusteredVariantDeprecationWriter =
                new ClusteredVariantDeprecationWriter(this.assemblyAccession,
                                                      this.mongoTemplate,
                                                      submittedVariantAccessioningService,
                                                      accessioningMonotonicInitRs,
                                                      this.deprecationIdSuffix,
                                                      this.deprecationReason);
    }

    @Override
    public void write(List<? extends SubmittedVariantEntity> svesToDeprecate) {
        List<? extends SubmittedVariantEntity> svesToDeprecateInSVE = svesToDeprecate.stream().filter(
                sve -> (sve.getAccession() >= accessioningMonotonicInitSs)).collect(Collectors.toList());
        List<? extends SubmittedVariantEntity> svesToDeprecateInDbsnpSVE = svesToDeprecate.stream().filter(
                sve -> (sve.getAccession() < accessioningMonotonicInitSs)).collect(Collectors.toList());
        deprecateVariants(svesToDeprecateInSVE, SubmittedVariantEntity.class);
        deprecateVariants(svesToDeprecateInDbsnpSVE, DbsnpSubmittedVariantEntity.class);
    }

    private void deprecateVariants(List<? extends SubmittedVariantEntity> svesToDeprecate,
                           Class<? extends SubmittedVariantEntity> sveCollectionToUse) {
        if (svesToDeprecate.size() > 0) {
            List<String> ssHashesToRemove = svesToDeprecate.stream().map(AccessionedDocument::getId).collect(
                    Collectors.toList());
            Set<ImmutablePair<String, Long>> associatedRSHashesAndIDs = svesToDeprecate.stream()
                    .filter(sve -> Objects.nonNull(sve.getClusteredVariantAccession()))
                    .map(EVAObjectModelUtils::toClusteredVariantEntity)
                    .map(cve -> new ImmutablePair<>(cve.getHashedMessage(), cve.getAccession()))
                    .collect(Collectors.toSet());
            List<ClusteredVariantEntity> cvesToDeprecate = this.clusteredVariantAccessioningService
                    .getAllActiveByAssemblyAndAccessionIn(this.assemblyAccession,
                            associatedRSHashesAndIDs.stream().map(c -> c.right).collect(Collectors.toList()))
                    .stream()
                    .filter(result ->
                            associatedRSHashesAndIDs.contains(
                                    new ImmutablePair<>(result.getHash(), result.getAccession())))
                    .map(result -> new ClusteredVariantEntity(result.getAccession(), result.getHash(),
                                                              result.getData()))
                    .collect(Collectors.toList());
            Class<? extends EventDocument<ISubmittedVariant, Long, ? extends SubmittedVariantInactiveEntity>>
                    svoeCollectionToUse = sveCollectionToUse.equals(SubmittedVariantEntity.class) ?
                    SubmittedVariantOperationEntity.class : DbsnpSubmittedVariantOperationEntity.class;
            writeDeprecationOperation(svesToDeprecate, svoeCollectionToUse);
            List<? extends SubmittedVariantEntity> removedEntities = this.mongoTemplate.findAllAndRemove(
                    query(where("_id").in(ssHashesToRemove)), sveCollectionToUse);
            this.numDeprecatedSubmittedEntities += removedEntities.size();
            this.clusteredVariantDeprecationWriter.write(cvesToDeprecate);
        }
    }

    private void writeDeprecationOperation(List<? extends SubmittedVariantEntity> svesToDeprecate,
                                   Class<? extends EventDocument<ISubmittedVariant, Long,
                                           ? extends SubmittedVariantInactiveEntity>> svoeCollectionToUse) {
        List<SubmittedVariantOperationEntity> svoesToWrite = svesToDeprecate.stream().map(sve -> {
            SubmittedVariantOperationEntity svoe = new SubmittedVariantOperationEntity();
            svoe.fill(EventType.DEPRECATED, sve.getAccession(), null,
                      this.deprecationReason,
                      Collections.singletonList(new SubmittedVariantInactiveEntity(sve)));
            svoe.setId(String.join("_", Arrays.asList("SS_DEPRECATED", this.deprecationIdSuffix, sve.getId())));
            return svoe;
        }).collect(Collectors.toList());
        Set<String> operationIds = svoesToWrite.stream().map(EventDocument::getId).collect(Collectors.toSet());
        Set<String> alreadyExistingIds = this.mongoTemplate.find(query(where("_id").in(operationIds)),
                                                                 svoeCollectionToUse).stream().map(EventDocument::getId)
                                                           .collect(Collectors.toSet());
        operationIds.removeAll(alreadyExistingIds);
        svoesToWrite = svoesToWrite.stream().filter(svoe -> operationIds.contains(svoe.getId())).collect(
                Collectors.toList());
        this.mongoTemplate.insert(svoesToWrite, svoeCollectionToUse);
    }

    public int getNumDeprecatedSubmittedEntities() {
        return numDeprecatedSubmittedEntities;
    }

    public int getNumDeprecatedClusteredEntities() {
        return this.clusteredVariantDeprecationWriter.getNumDeprecatedEntities();
    }
}
