package uk.ac.ebi.eva.accession.dbsnp.contig;

public class ContigMapping {

    private ContigMapWrapper contigMapWrapper;

    public ContigMapping(String mappingUrl) throws Exception {
        this(new AssemblyReportParser(mappingUrl).fillContigMap());
    }

    public ContigMapping(ContigMapWrapper contigMap) {
        this.contigMapWrapper = contigMap;
    }

    public String getContigOrDefault(String contig, ContigNameConvention contigNameConvention) {
        return contigMapWrapper.getSynonymByContigConvention(contig, contigNameConvention);
    }

    public ContigSynonyms getContigSynonyms(String contig) {
        return contigMapWrapper.getSynonyms(contig);
    }
}
