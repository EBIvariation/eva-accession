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

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.info.BuildProperties;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

@Document
public class RemappingMetadata {

    @Autowired
    BuildProperties buildProperties;

    @Id
    private String hashedMessage;

    private String remappingVersion;

//    @Value("${build.version}")
    private String ingestionVersion;

    private String remappedFrom;

    private String remappedTo;

    public RemappingMetadata(String remappingVersion, String ingestionVersion, String remappedFrom, String remappedTo) {
        this.remappingVersion = remappingVersion;
        this.ingestionVersion = ingestionVersion;
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

    public String getIngestionVersion() {
        return ingestionVersion;
    }

    public void setIngestionVersion(String ingestionVersion) {
        this.ingestionVersion = ingestionVersion;
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
