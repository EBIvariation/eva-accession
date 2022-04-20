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

@JsonIgnoreProperties(ignoreUnknown = true)
public class ContigAliasAssembly {

    private long id;

    private String name;

    private String organism;

    private Long taxid;

    private String genbank;

    private String refseq;

    private boolean isGenbankRefseqIdentical;

    private String md5checksum;

    private String trunc512checksum;

    public ContigAliasAssembly() {
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getOrganism() {
        return organism;
    }

    public void setOrganism(String organism) {
        this.organism = organism;
    }

    public Long getTaxid() {
        return taxid;
    }

    public void setTaxid(Long taxid) {
        this.taxid = taxid;
    }

    public String getGenbank() {
        return genbank;
    }

    public void setGenbank(String genbank) {
        this.genbank = genbank;
    }

    public String getRefseq() {
        return refseq;
    }

    public void setRefseq(String refseq) {
        this.refseq = refseq;
    }

    public boolean isGenbankRefseqIdentical() {
        return isGenbankRefseqIdentical;
    }

    public void setGenbankRefseqIdentical(boolean genbankRefseqIdentical) {
        isGenbankRefseqIdentical = genbankRefseqIdentical;
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
}
