package uk.ac.ebi.eva.accession.ws.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionVersionsWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService;
import uk.ac.ebi.eva.accession.ws.service.dbsnp.DbsnpReadOnlyClusteredVariantService;
import uk.ac.ebi.eva.accession.ws.service.eva.EvaReadOnlyClusteredVariantService;
import uk.ac.ebi.eva.commons.core.models.VariantType;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Read-only version of {@link uk.ac.ebi.eva.accession.core.service.nonhuman.ClusteredVariantAccessioningService}
 */
public class ReadOnlyClusteredVariantService implements AccessioningService<IClusteredVariant, String, Long> {

    private static Logger logger = LoggerFactory.getLogger(ReadOnlyClusteredVariantService.class);

    private EvaReadOnlyClusteredVariantService accessioningService;

    private DbsnpReadOnlyClusteredVariantService accessioningServiceDbsnp;

    private Long accessioningMonotonicInitRs;

    private final ContigAliasService contigAliasService;

    public ReadOnlyClusteredVariantService(EvaReadOnlyClusteredVariantService accessioningService,
                                           DbsnpReadOnlyClusteredVariantService accessioningServiceDbsnp,
                                           Long accessioningMonotonicInitRs,
                                           ContigAliasService contigAliasService) {
        this.accessioningService = accessioningService;
        this.accessioningServiceDbsnp = accessioningServiceDbsnp;
        this.accessioningMonotonicInitRs = accessioningMonotonicInitRs;
        this.contigAliasService = contigAliasService;
    }

    private List<AccessionWrapper<IClusteredVariant, String, Long>> joinLists(
            List<AccessionWrapper<IClusteredVariant, String, Long>> l1,
            List<AccessionWrapper<IClusteredVariant, String, Long>> l2) {
        l1.addAll(l2);
        return l1;
    }

    @Override
    public List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> getOrCreate(List<? extends IClusteredVariant> messages, String applicationInstanceId) {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public List<AccessionWrapper<IClusteredVariant, String, Long>> get(List<? extends IClusteredVariant> variants) {
        return joinLists(accessioningService.get(variants), accessioningServiceDbsnp.get(variants));
    }

    /**
     * Conceptually, for remapped variants or variants imported from dbSNP, a single accession could return several
     * documents.
     * This method is implemented to comply with the accession-commons interface but will only return one variant,
     * to get all the variants use {@link #getAllByAccession}.
     */
    @Override
    public AccessionWrapper<IClusteredVariant, String, Long> getByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        if (accession >= accessioningMonotonicInitRs) {
            return accessioningService.getByAccession(accession);
        } else {
            return accessioningServiceDbsnp.getByAccession(accession);
        }
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getAllByAccession(
            Long accession, ContigNamingConvention contigNamingConvention)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        List<AccessionWrapper<IClusteredVariant, String, Long>> clusteredVariants;
        if (accession >= accessioningMonotonicInitRs) {
            clusteredVariants = accessioningService.getAllByAccession(accession);
        } else {
            clusteredVariants = accessioningServiceDbsnp.getAllByAccession(accession);
        }
        return contigAliasService.getClusteredVariantsWithTranslatedContig(clusteredVariants, contigNamingConvention);
    }

    @Override
    public AccessionWrapper<IClusteredVariant, String, Long> getByAccessionAndVersion(Long accession, int version)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        if (accession >= accessioningMonotonicInitRs) {
            return accessioningService.getByAccessionAndVersion(accession, version);
        } else {
            return accessioningServiceDbsnp.getByAccessionAndVersion(accession, version);
        }
    }

    @Override
    public AccessionVersionsWrapper<IClusteredVariant, String, Long> update(Long aLong, int version, IClusteredVariant message) {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public AccessionVersionsWrapper<IClusteredVariant, String, Long> patch(Long aLong, IClusteredVariant message) {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public void deprecate(Long aLong, String reason) {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public void merge(Long accessionOrigin, Long mergeInto, String reason) {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getByIdFields(
            String assembly, String contig, long start, VariantType type, ContigNamingConvention contigNamingConvention) {
        String insdcContig = contigAliasService.translateContigToInsdc(contig, assembly, contigNamingConvention);
        IClusteredVariant clusteredVariant = new ClusteredVariant(assembly, 0, insdcContig, start, type, false, null);
        List<AccessionWrapper<IClusteredVariant, String, Long>> variants = this.get(
                Collections.singletonList(clusteredVariant));
        return variants
                .stream()
                .map(accessionWrapper -> contigAliasService.createClusteredVariantAccessionWrapperWithNewContig(accessionWrapper, contig))
                .collect(Collectors.toList());
    }

    public AccessionWrapper<IClusteredVariant, String, Long> getLastInactive(Long accession) {
        if (accession >= accessioningMonotonicInitRs) {
            return accessioningService.getLastInactive(accession);
        } else {
            return accessioningServiceDbsnp.getLastInactive(accession);
        }
    }

}
