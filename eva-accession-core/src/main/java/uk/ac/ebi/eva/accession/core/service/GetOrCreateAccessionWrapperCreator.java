package uk.ac.ebi.eva.accession.core.service;

import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;

import java.util.List;
import java.util.stream.Collectors;

public class GetOrCreateAccessionWrapperCreator {
    public static <MODEL> List<GetOrCreateAccessionWrapper<MODEL, String, Long>>
    convertToGetOrCreateAccessionWrapper(List<AccessionWrapper<MODEL, String, Long>> accessionWrapper) {
        return accessionWrapper.stream().map(wrapper -> new GetOrCreateAccessionWrapper<>(wrapper.getAccession(),
                                                                                          wrapper.getHash(),
                                                                                          wrapper.getData(),
                                                                                          false))
                               .collect(Collectors.toList());
    }
}
