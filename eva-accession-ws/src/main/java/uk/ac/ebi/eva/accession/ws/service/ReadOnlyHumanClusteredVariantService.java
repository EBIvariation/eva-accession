package uk.ac.ebi.eva.accession.ws.service;


import org.springframework.beans.factory.annotation.Qualifier;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.service.human.dbsnp.HumanDbsnpClusteredVariantOperationAccessioningService;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class ReadOnlyHumanClusteredVariantService {

    private final DbsnpReadOnlyHumanClusteredVariantService humanService;

    private final HumanDbsnpClusteredVariantOperationAccessioningService operationsService;

    private final ContigAliasService contigAliasService;

    public ReadOnlyHumanClusteredVariantService(
            @Qualifier("humanActiveService") DbsnpReadOnlyHumanClusteredVariantService humanService,
            @Qualifier("humanOperationsService") HumanDbsnpClusteredVariantOperationAccessioningService operationsService,
            ContigAliasService contigAliasService) {
        this.humanService = humanService;
        this.operationsService = operationsService;
        this.contigAliasService = contigAliasService;
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getAllByAccession(Long identifier) {
        return getAllByAccession(identifier, ContigNamingConvention.INSDC);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getAllByAccession(Long identifier, ContigNamingConvention contigNamingConvention) {
        List<AccessionWrapper<IClusteredVariant, String, Long>> clusteredVariants = new ArrayList<>();
        clusteredVariants.addAll(contigAliasService.getClusteredVariantsWithTranslatedContig(getHumanClusteredVariants(identifier),
                contigNamingConvention));
        clusteredVariants.addAll(contigAliasService.getClusteredVariantsWithTranslatedContig(operationsService.getByAccession(identifier),
                contigNamingConvention));
        return clusteredVariants;
    }

    private List<AccessionWrapper<IClusteredVariant, String, Long>> getHumanClusteredVariants(Long identifier) {
        try {
            AccessionWrapper<IClusteredVariant, String, Long> wrapper = humanService.getByAccession(identifier);
            return Collections.singletonList(wrapper);
        } catch (AccessionDoesNotExistException | AccessionMergedException | AccessionDeprecatedException e) {
            return Collections.emptyList();
        }
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getByIdFields(
            String assembly, String contig, long start, VariantType type, ContigNamingConvention contigNamingConvention) {
        List<AccessionWrapper<IClusteredVariant, String, Long>> clusteredVariants = new ArrayList<>();

        String insdcContig = contigAliasService.translateContigToInsdc(contig, assembly, contigNamingConvention);
        IClusteredVariant clusteredVariantToSearch = new ClusteredVariant(assembly, 0, insdcContig, start, type, false,
                null);

        List<AccessionWrapper<IClusteredVariant, String, Long>> activeClusteredVariantWrappers =
                humanService.get(Collections.singletonList(clusteredVariantToSearch));
        clusteredVariants.addAll(activeClusteredVariantWrappers);

        List<AccessionWrapper<IClusteredVariant, String, Long>> activeClusteredVariantsInOperations =
                operationsService.getOriginalVariant(clusteredVariantToSearch);
        clusteredVariants.addAll(activeClusteredVariantsInOperations);

        return clusteredVariants
                .stream()
                .map(accessionWrapper -> contigAliasService.createClusteredVariantAccessionWrapperWithNewContig(accessionWrapper, contig))
                .collect(Collectors.toList());
    }
}
