package uk.ac.ebi.eva.accession.pipeline.steps.processors;

import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.SubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.IVariant;

public class VariantProcessor implements ItemProcessor<IVariant, SubmittedVariant> {

    private String assemblyAccession;

    private String taxonomyAccession;

    private String projectAccession;

    public VariantProcessor(String assemblyAccession, String taxonomyAccession, String projectAccession) {
        this.assemblyAccession = assemblyAccession;
        this.taxonomyAccession = taxonomyAccession;
        this.projectAccession = projectAccession;
    }

    @Override
    public SubmittedVariant process(final IVariant variant) throws Exception {

        return new SubmittedVariant(assemblyAccession, taxonomyAccession, projectAccession,
                variant.getChromosome(), variant.getStart(), variant.getReference(),
                variant.getAlternate(), true);
    }
}
