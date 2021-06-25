/*
 * Copyright 2021 EMBL - European Bioinformatics Institute
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
package uk.ac.ebi.eva.remapping.ingest.batch.tasklets;

import org.springframework.data.annotation.Id;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

public class RemappingMetadata {

    @Id
    private String hashedMessage;

    private String remappingVersion;

    private String accessioningVersion;

    private String remappedFrom;

    private String remappedTo;

    public RemappingMetadata(String remappingVersion, String accessioningVersion, String remappedFrom, String remappedTo) {
        this.remappingVersion = remappingVersion;
        this.accessioningVersion = accessioningVersion;
        this.remappedFrom = remappedFrom;
        this.remappedTo = remappedTo;
        this.hashedMessage = new RemappingMetadataSummaryFunction().andThen(new SHA1HashingFunction()).apply(this);
    }

    public String getHashedMessage() {
        return hashedMessage;
    }

    public void setHashedMessage() {
        this.hashedMessage = hashedMessage;
    }

    public String getRemappingVersion() {
        return remappingVersion;
    }

    public void setRemappingVersion(String remappingVersion) {
        this.remappingVersion = remappingVersion;
    }

    public String getAccessioningVersion() {
        return accessioningVersion;
    }

    public void setAccessioningVersion(String accessioningVersion) {
        this.accessioningVersion = accessioningVersion;
    }

    public String getRemappedFrom() {
        return remappedFrom;
    }

    public void setRemappedFrom(String remappedFrom) {
        this.remappedFrom = remappedFrom;
    }

    public String getRemappedTo() {
        return remappedTo;
    }

    public void setRemappedTo(String remappedTo) {
        this.remappedTo = remappedTo;
    }
}
