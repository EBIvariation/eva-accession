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
package uk.ac.ebi.eva.accession.core.model.eva;

import org.springframework.data.mongodb.core.mapping.Document;
import uk.ac.ebi.ampt2d.commons.accession.persistence.mongodb.document.EventDocument;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;

@Document
public class SubmittedVariantOperationEntity extends EventDocument<ISubmittedVariant, Long,
        SubmittedVariantInactiveEntity> {

    @Override
    public String toString() {
        return "ClusteredVariantOperationEntity{"
                + "id='" + getId() + '\''
                + ", eventType=" + getEventType()
                + ", accession=" + getAccession()
                + ", mergedInto=" + getMergedInto()
                + ", reason='" + getReason() + '\''
                + ", createdDate=" + getCreatedDate()
                + ", inactiveObjects=" + getInactiveObjects()
                + '}';
    }
}
