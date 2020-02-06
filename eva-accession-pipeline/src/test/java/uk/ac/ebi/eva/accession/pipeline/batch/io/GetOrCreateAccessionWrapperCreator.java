package uk.ac.ebi.eva.accession.pipeline.batch.io;

import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;

import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;

import java.util.List;
import java.util.stream.Collectors;

class GetOrCreateAccessionWrapperCreator {
    protected static List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>>
    convertToGetOrCreateAccessionWrapper(List<AccessionWrapper<ISubmittedVariant, String, Long>> accessionWrapper) {
        return accessionWrapper.stream().map(wrapper -> new GetOrCreateAccessionWrapper<>(wrapper.getAccession(),
                                                                                          wrapper.getHash(),
                                                                                          wrapper.getData(),
                                                                                          false))
                               .collect(Collectors.toList());
    }
}
