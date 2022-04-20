/*
 *
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
 *
 */
package uk.ac.ebi.eva.accession.core.contigalias;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonIgnoreProperties(ignoreUnknown = true)
public class ContigAliasChromosome {

    private long id;

    private String genbankSequenceName;

    private String enaSequenceName;

    @JsonProperty("genbank")
    private String insdc;

    private String refseq;

    private String ucscName;

    private String md5checksum;

    private String trunc512checksum;

    private ContigAliasAssembly assembly;

    public ContigAliasChromosome() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getGenbankSequenceName() {
        return genbankSequenceName;
    }

    public void setGenbankSequenceName(String genbankSequenceName) {
        this.genbankSequenceName = genbankSequenceName;
    }

    public String getEnaSequenceName() {
        return enaSequenceName;
    }

    public void setEnaSequenceName(String enaSequenceName) {
        this.enaSequenceName = enaSequenceName;
    }

    public String getInsdc() {
        return insdc;
    }

    public void setInsdc(String insdc) {
        this.insdc = insdc;
    }

    public String getRefseq() {
        return refseq;
    }

    public void setRefseq(String refseq) {
        this.refseq = refseq;
    }

    public String getUcscName() {
        return ucscName;
    }

    public void setUcscName(String ucscName) {
        this.ucscName = ucscName;
    }

    public String getMd5checksum() {
        return md5checksum;
    }

    public void setMd5checksum(String md5checksum) {
        this.md5checksum = md5checksum;
    }

    public String getTrunc512checksum() {
        return trunc512checksum;
    }

    public void setTrunc512checksum(String trunc512checksum) {
        this.trunc512checksum = trunc512checksum;
    }

    public ContigAliasAssembly getAssembly() {
        return assembly;
    }

    public void setAssembly(ContigAliasAssembly assembly) {
        this.assembly = assembly;
    }
}
