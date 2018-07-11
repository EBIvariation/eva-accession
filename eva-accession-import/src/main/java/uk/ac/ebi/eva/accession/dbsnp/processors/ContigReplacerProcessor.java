package uk.ac.ebi.eva.accession.dbsnp.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.dbsnp.contig.ContigMapping;
import uk.ac.ebi.eva.accession.dbsnp.contig.ContigSynonyms;
import uk.ac.ebi.eva.accession.dbsnp.model.SubSnpNoHgvs;

public class ContigReplacerProcessor implements ItemProcessor<SubSnpNoHgvs, SubSnpNoHgvs> {

    private ContigMapping contigMapping;

    public ContigReplacerProcessor(ContigMapping contigMapping) {
        this.contigMapping = contigMapping;
    }

    @Override
    public SubSnpNoHgvs process(SubSnpNoHgvs subSnpNoHgvs) throws Exception {
        ContigSynonyms contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getContigName());
        if (contigSynonyms != null) {
            subSnpNoHgvs.setContigName(contigSynonyms.getSequenceName());
            return subSnpNoHgvs;
        }
        throw new IllegalArgumentException(
                "Contig '" + subSnpNoHgvs.getContigName() + "' not found in the ASSEMBLY REPORT");
    }
}
