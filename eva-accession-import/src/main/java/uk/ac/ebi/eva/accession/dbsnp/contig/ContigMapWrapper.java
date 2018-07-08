package uk.ac.ebi.eva.accession.dbsnp.contig;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ContigMapWrapper {

    private Map<String, ContigSynonyms> SequenceNameMap;

    private Map<String, ContigSynonyms> GenBankMap;

    private Map<String, ContigSynonyms> RefSeqMap;

    private Map<String, ContigSynonyms> UcscMap;

    private static final String CHROMOSOME_PATTERN = "(chromosome|chrom|chr|ch)(.+)";

    private static final Pattern PATTERN = Pattern.compile(CHROMOSOME_PATTERN, Pattern.CASE_INSENSITIVE);

    public ContigMapWrapper() {
        SequenceNameMap = new HashMap<>();
        GenBankMap = new HashMap<>();
        RefSeqMap = new HashMap<>();
        UcscMap = new HashMap<>();
    }

    public ContigMapWrapper(
            Map<String, ContigSynonyms> sequenceNameMap,
            Map<String, ContigSynonyms> genBankMap,
            Map<String, ContigSynonyms> refSeqMap,
            Map<String, ContigSynonyms> ucscMap) {
        SequenceNameMap = sequenceNameMap;
        GenBankMap = genBankMap;
        RefSeqMap = refSeqMap;
        UcscMap = ucscMap;
    }

    public Map<String, ContigSynonyms> getSequenceNameMap() {
        return SequenceNameMap;
    }

    public void setSequenceNameMap(
            Map<String, ContigSynonyms> sequenceNameMap) {
        SequenceNameMap = sequenceNameMap;
    }

    public Map<String, ContigSynonyms> getGenBankMap() {
        return GenBankMap;
    }

    public void setGenBankMap(Map<String, ContigSynonyms> genBankMap) {
        GenBankMap = genBankMap;
    }

    public Map<String, ContigSynonyms> getRefSeqMap() {
        return RefSeqMap;
    }

    public void setRefSeqMap(Map<String, ContigSynonyms> refSeqMap) {
        RefSeqMap = refSeqMap;
    }

    public Map<String, ContigSynonyms> getUcscMap() {
        return UcscMap;
    }

    public void setUcscMap(Map<String, ContigSynonyms> ucscMap) {
        UcscMap = ucscMap;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ContigMapWrapper that = (ContigMapWrapper) o;
        return Objects.equals(SequenceNameMap, that.SequenceNameMap) &&
                Objects.equals(GenBankMap, that.GenBankMap) &&
                Objects.equals(RefSeqMap, that.RefSeqMap) &&
                Objects.equals(UcscMap, that.UcscMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(SequenceNameMap, GenBankMap, RefSeqMap, UcscMap);
    }

    public ContigSynonyms getSynonyms(String contig) {
        contig = removePrefix(contig);
        ContigSynonyms contigSynonyms;
        if ((contigSynonyms = SequenceNameMap.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = GenBankMap.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = RefSeqMap.get(contig)) != null) {
            return contigSynonyms;
        }
        if ((contigSynonyms = UcscMap.get(contig)) != null) {
            return contigSynonyms;
        }
        return null;
    }

    private String removePrefix(String contig) {
        Matcher matcher = PATTERN.matcher(contig);
        String contigNoPrefix = contig;
        if (matcher.matches()) {
            contigNoPrefix = matcher.group(2);
        }
        return contigNoPrefix;
    }

    public void fillContigConventionMaps(ContigSynonyms contigSynonyms) {
        contigSynonyms.setSequenceName(removePrefix(contigSynonyms.getSequenceName()));
        contigSynonyms.setUcsc(removePrefix(contigSynonyms.getUcsc()));
        SequenceNameMap.put(contigSynonyms.getSequenceName(), contigSynonyms);
        GenBankMap.put(contigSynonyms.getGenBank(), contigSynonyms);
        RefSeqMap.put(contigSynonyms.getRefSeq(), contigSynonyms);
        UcscMap.put(contigSynonyms.getUcsc(), contigSynonyms);
    }
}
