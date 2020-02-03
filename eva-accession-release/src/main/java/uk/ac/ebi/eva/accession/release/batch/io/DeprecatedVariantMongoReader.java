/*
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
 */
package uk.ac.ebi.eva.accession.release.batch.io;

import org.springframework.data.mongodb.core.MongoTemplate;
import uk.ac.ebi.ampt2d.commons.accession.core.models.EventType;

import uk.ac.ebi.eva.accession.core.model.dbsnp.DbsnpClusteredVariantOperationEntity;

public class DeprecatedVariantMongoReader extends MongoDbCursorItemReader<DbsnpClusteredVariantOperationEntity> {

    private static final String DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY = "dbsnpClusteredVariantOperationEntity";

    public DeprecatedVariantMongoReader(String assemblyAccession, MongoTemplate mongoTemplate) {
        setCollection(DBSNP_CLUSTERED_VARIANT_OPERATION_ENTITY);
        setTemplate(mongoTemplate);
        setTargetType(DbsnpClusteredVariantOperationEntity.class);

        setQuery(String.format("{ \"inactiveObjects.asm\" : \"%s\", eventType : \"%s\" }", assemblyAccession,
                 EventType.DEPRECATED));
    }

}
