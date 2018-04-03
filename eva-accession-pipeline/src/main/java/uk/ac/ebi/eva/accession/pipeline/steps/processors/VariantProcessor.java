package uk.ac.ebi.eva.accession.pipeline.steps.processors;


import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.persistence.SubmittedVariantEntity;
import uk.ac.ebi.eva.commons.core.models.IVariant;

public class VariantProcessor implements ItemProcessor<IVariant, SubmittedVariantEntity> {

    @Override
    public SubmittedVariantEntity process(IVariant variant) throws Exception {
        // TODO: implement
        return null;
    }
}
