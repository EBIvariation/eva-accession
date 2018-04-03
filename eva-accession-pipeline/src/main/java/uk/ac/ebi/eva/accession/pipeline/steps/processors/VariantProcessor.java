package uk.ac.ebi.eva.accession.pipeline.steps.processors;


import org.springframework.batch.item.ItemProcessor;

import uk.ac.ebi.eva.accession.core.VariantModel;
import uk.ac.ebi.eva.commons.core.models.pipeline.Variant;

public class VariantProcessor implements ItemProcessor<Variant, VariantModel> {

    @Override
    public VariantModel process(Variant variant) throws Exception {
        // TODO: implement
        return null;
    }
}
