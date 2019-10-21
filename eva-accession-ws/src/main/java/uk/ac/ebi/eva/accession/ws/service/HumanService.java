package uk.ac.ebi.eva.accession.ws.service;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;

import uk.ac.ebi.eva.accession.core.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.core.IClusteredVariant;

import java.util.Collections;
import java.util.List;

@Service
public class HumanService {

    private ClusteredVariantAccessioningService clusteredHumanVariantAccessioningService;

    public HumanService(@Qualifier("humanService1") ClusteredVariantAccessioningService clusteredVariantAccessioningService) {
        this.clusteredHumanVariantAccessioningService = clusteredVariantAccessioningService;
    }

    public List<AccessionResponseDTO<ClusteredVariant, IClusteredVariant, String, Long>> getRs(Long id)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        AccessionWrapper<IClusteredVariant, String, Long> wrapper = clusteredHumanVariantAccessioningService.getByAccession(id);
        return Collections.singletonList(new AccessionResponseDTO<>(wrapper, ClusteredVariant::new));
    }
}
