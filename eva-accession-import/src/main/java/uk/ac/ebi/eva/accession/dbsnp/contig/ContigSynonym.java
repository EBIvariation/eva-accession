package uk.ac.ebi.eva.accession.dbsnp.contig;

import java.util.Objects;

public class ContigSynonym {

    private String sequenceName;

    private String genBank;

    private String refSeq;

    private String ucsc;

    public ContigSynonym(String sequenceName, String genBank, String refSeq, String ucsc) {
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContigSynonym that = (ContigSynonym) o;
        return Objects.equals(sequenceName, that.sequenceName) &&
                Objects.equals(genBank, that.genBank) &&
                Objects.equals(refSeq, that.refSeq) &&
                Objects.equals(ucsc, that.ucsc);
    }

    @Override
    public int hashCode() {

        return Objects.hash(sequenceName, genBank, refSeq, ucsc);
    }
}