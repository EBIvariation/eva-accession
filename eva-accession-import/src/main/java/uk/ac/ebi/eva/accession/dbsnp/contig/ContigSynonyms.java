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
package uk.ac.ebi.eva.accession.dbsnp.contig;

public class ContigSynonyms {

    private String sequenceName;

    private String genBank;

    private String refSeq;

    private String ucsc;

    public ContigSynonyms(String sequenceName, String genBank, String refSeq, String ucsc) {
        this.sequenceName = sequenceName;
        this.genBank = genBank;
        this.refSeq = refSeq;
        this.ucsc = ucsc;
    }

    public String getSequenceName() {
        return sequenceName;
    }

    public void setSequenceName(String sequenceName) {
        this.sequenceName = sequenceName;
    }

    public String getGenBank() {
        return genBank;
    }

    public void setGenBank(String genBank) {
        this.genBank = genBank;
    }

    public String getRefSeq() {
        return refSeq;
    }

    public void setRefSeq(String refSeq) {
        this.refSeq = refSeq;
    }

    public String getUcsc() {
        return ucsc;
    }

    public void setUcsc(String ucsc) {
        this.ucsc = ucsc;
    }

    @Override
    public String toString() {
        return "ContigSynonyms{" +
                "sequenceName='" + sequenceName + '\'' +
                ", genBank='" + genBank + '\'' +
                ", refSeq='" + refSeq + '\'' +
                ", ucsc='" + ucsc + '\'' +
                '}';
    }
}