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
        ContigSynonyms contigSynonyms;
        if (subSnpNoHgvs.getChromosome() != null) {
            if ((contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getChromosome())) != null) {
                subSnpNoHgvs.setChromosome(contigSynonyms.getSequenceName());
            }
        } else {
            contigSynonyms = contigMapping.getContigSynonyms(subSnpNoHgvs.getContigName());
        }

        if (contigSynonyms == null) {
            throw new IllegalArgumentException(
                    "Contig '" + subSnpNoHgvs.getContigName() + "' not found in the assembly report");
        }

        subSnpNoHgvs.setContigName(contigSynonyms.getSequenceName());
        return subSnpNoHgvs;
    }
}
