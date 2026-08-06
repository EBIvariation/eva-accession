package uk.ac.ebi.eva.accession.ws.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import uk.ac.ebi.ampt2d.commons.accession.core.AccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.HashAlreadyExistsException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionVersionsWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.GetOrCreateAccessionWrapper;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.model.SubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.SubmittedVariantAccessioningService;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

public class ReadOnlySubmittedVariantService implements AccessioningService<ISubmittedVariant, String, Long> {

    private static Logger logger = LoggerFactory.getLogger(SubmittedVariantAccessioningService.class);

    private EvaReadOnlySubmittedVariantService accessioningService;

    private DbsnpReadOnlySubmittedVariantService accessioningServiceDbsnp;

    private Long accessioningMonotonicInitSs;

    private ContigAliasService contigAliasService;

    public ReadOnlySubmittedVariantService(EvaReadOnlySubmittedVariantService accessioningService,
                                           DbsnpReadOnlySubmittedVariantService accessioningServiceDbsnp,
                                           Long accessioningMonotonicInitSs,
                                           ContigAliasService contigAliasService) {
        this.accessioningService = accessioningService;
        this.accessioningServiceDbsnp = accessioningServiceDbsnp;
        this.accessioningMonotonicInitSs = accessioningMonotonicInitSs;
        this.contigAliasService = contigAliasService;
    }


    private List<AccessionWrapper<ISubmittedVariant, String, Long>> joinLists(
            List<AccessionWrapper<ISubmittedVariant, String, Long>> l1,
            List<AccessionWrapper<ISubmittedVariant, String, Long>> l2) {
        l1.addAll(l2);
        return l1;
    }

    @Override
    public List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> getOrCreate(List<? extends ISubmittedVariant> messages, String applicationInstanceId) throws AccessionCouldNotBeGeneratedException {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public List<AccessionWrapper<ISubmittedVariant, String, Long>> get(List<? extends ISubmittedVariant> variants) {
        return joinLists(accessioningService.get(variants), accessioningServiceDbsnp.get(variants));
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getAllByIdFields(
            String assembly, String contig, List<String> studies, long start, String reference, String alternate,
            ContigNamingConvention contigNamingConvention) {
        String insdcContig = contigAliasService.translateContigToInsdc(contig, assembly, contigNamingConvention);
        List<SubmittedVariant> submittedVariants = studies.stream()
                .map(study -> new SubmittedVariant(assembly, 0, study, insdcContig, start, reference, alternate, null))
                .collect(Collectors.toList());
        List<AccessionWrapper<ISubmittedVariant, String, Long>> variants = this.get(submittedVariants);
        return variants
                .stream()
                .map(accessionWrapper -> contigAliasService.createSubmittedVariantAccessionWrapperWithNewContig(accessionWrapper, contig))
                .collect(Collectors.toList());
    }

    /**
     * Conceptually, for variants imported from dbSNP, a single accession could return several documents.
     * This method is implemented to comply with the accession-commons interface, but will only return one variant,
     * to get all the variants use {@link #getAllByAccession}.
     */
    @Override
    public AccessionWrapper<ISubmittedVariant, String, Long> getByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        if (accession >= accessioningMonotonicInitSs) {
            return accessioningService.getByAccession(accession);
        } else {
            return accessioningServiceDbsnp.getByAccession(accession);
        }
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getAllByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        return getAllByAccession(accession, ContigNamingConvention.INSDC);
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getAllByAccession(
            Long accession, ContigNamingConvention contigNamingConvention) throws AccessionMergedException,
            AccessionDoesNotExistException, AccessionDeprecatedException, NoSuchElementException {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> submittedVariants;
        if (accession >= accessioningMonotonicInitSs) {
            submittedVariants = accessioningService.getAllByAccession(accession);
        } else {
            submittedVariants = accessioningServiceDbsnp.getAllByAccession(accession);
        }
        return contigAliasService.getSubmittedVariantsWithTranslatedContig(submittedVariants, contigNamingConvention);
    }

    @Override
    public AccessionWrapper<ISubmittedVariant, String, Long> getByAccessionAndVersion(Long accession, int version)
            throws AccessionDoesNotExistException, AccessionMergedException, AccessionDeprecatedException {
        if (accession >= accessioningMonotonicInitSs) {
            return accessioningService.getByAccessionAndVersion(accession, version);
        } else {
            return accessioningServiceDbsnp.getByAccessionAndVersion(accession, version);
        }
    }

    @Override
    public AccessionVersionsWrapper<ISubmittedVariant, String, Long> update(Long aLong, int version, ISubmittedVariant message) throws AccessionDoesNotExistException, HashAlreadyExistsException, AccessionDeprecatedException, AccessionMergedException {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public AccessionVersionsWrapper<ISubmittedVariant, String, Long> patch(Long aLong, ISubmittedVariant message) throws AccessionDoesNotExistException, HashAlreadyExistsException, AccessionDeprecatedException, AccessionMergedException {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public void deprecate(Long aLong, String reason) throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    @Override
    public void merge(Long accessionOrigin, Long mergeInto, String reason) throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        throw new UnsupportedOperationException("Not supported in read-only service");
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getByClusteredVariantAccessionIn(
            List<Long> clusteredVariantAccessions) {
        return getByClusteredVariantAccessionIn(clusteredVariantAccessions, ContigNamingConvention.INSDC);
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getByClusteredVariantAccessionIn(
            List<Long> clusteredVariantAccessions, ContigNamingConvention contigNamingConvention) {
        return joinLists(contigAliasService.getSubmittedVariantsWithTranslatedContig(accessioningService.getByClusteredVariantAccessionIn(clusteredVariantAccessions),
                        contigNamingConvention),
                contigAliasService.getSubmittedVariantsWithTranslatedContig(accessioningServiceDbsnp.getByClusteredVariantAccessionIn(clusteredVariantAccessions),
                        contigNamingConvention));
    }

    public AccessionWrapper<ISubmittedVariant, String, Long> getLastInactive(Long accession) {
        if (accession >= accessioningMonotonicInitSs) {
            return accessioningService.getLastInactive(accession);
        } else {
            return accessioningServiceDbsnp.getLastInactive(accession);
        }
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>>
    getAllActiveByAssemblyAndAccessionIn(String assembly, List<Long> accessionList) {
        List<Long> evaAccessions = new ArrayList<>();
        List<Long> dbsnpAccessions = new ArrayList<>();
        for (Long accession : accessionList) {
            if (accession >= accessioningMonotonicInitSs) {
                evaAccessions.add(accession);
            } else {
                dbsnpAccessions.add(accession);
            }
        }
        List<AccessionWrapper<ISubmittedVariant, String, Long>> result =
                accessioningService.getAllActiveByAssemblyAndAccessionIn(assembly, evaAccessions);
        result.addAll(accessioningServiceDbsnp.getAllActiveByAssemblyAndAccessionIn(assembly, dbsnpAccessions));
        return result;
    }

}
