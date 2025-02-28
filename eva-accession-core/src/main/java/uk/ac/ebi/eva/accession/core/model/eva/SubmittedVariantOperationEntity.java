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

import java.time.LocalDateTime;
import java.util.Objects;

@Document
public class SubmittedVariantOperationEntity extends EventDocument<ISubmittedVariant, Long,
        SubmittedVariantInactiveEntity> {

    public SubmittedVariantOperationEntity() {
        super();
        //Set the created date to assure is is assigned when performing bulk operations
        setCreatedDate(LocalDateTime.now());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SubmittedVariantOperationEntity that = (SubmittedVariantOperationEntity) o;
        return Objects.equals(this.getId(), that.getId()) &&
                Objects.equals(this.getEventType(), that.getEventType()) &&
                Objects.equals(this.getAccession(), that.getAccession()) &&
                Objects.equals(this.getMergedInto(), that.getMergedInto()) &&
                Objects.equals(this.getSplitInto(), that.getSplitInto()) &&
                Objects.equals(this.getReason(), that.getReason()) &&
                Objects.equals(this.getCreatedDate(), that.getCreatedDate()) &&
                Objects.equals(this.getInactiveObjects(), that.getInactiveObjects());
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.getId(), this.getEventType(), this.getAccession(), this.getMergedInto(),
                            this.getSplitInto(), this.getReason(), this.getCreatedDate(), this.getInactiveObjects());
    }

    @Override
    public String toString() {
        return "SubmittedVariantOperationEntity{"
                + "id='" + getId() + '\''
                + ", eventType=" + getEventType()
                + ", accession=" + getAccession()
                + ", mergedInto=" + getMergedInto()
                + ", splitInto=" + getSplitInto()
                + ", reason='" + getReason() + '\''
                + ", createdDate=" + getCreatedDate()
                + ", inactiveObjects=" + getInactiveObjects()
                + '}';
    }
}
