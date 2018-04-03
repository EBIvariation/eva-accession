package uk.ac.ebi.eva.accession.pipeline.steps.processors;


import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.ISubmittedVariant;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

public class VariantProcessor implements ItemProcessor<Variant, ISubmittedVariant> {

    @Override
    public ISubmittedVariant process(Variant variant) throws Exception {
        // TODO: implement
        return null;
    }
}
