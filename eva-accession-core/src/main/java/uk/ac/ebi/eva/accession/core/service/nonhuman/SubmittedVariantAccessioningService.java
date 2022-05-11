/*
 *
 * Copyright 2018 EMBL - European Bioinformatics Institute
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package uk.ac.ebi.eva.accession.core.service.nonhuman;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.util.Pair;
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
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpSubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.SubmittedVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.commons.core.models.contigalias.ContigNamingConvention;

import java.util.ArrayList;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.stream.Collectors;

public class SubmittedVariantAccessioningService implements AccessioningService<ISubmittedVariant, String, Long> {

    private static Logger logger = LoggerFactory.getLogger(SubmittedVariantAccessioningService.class);

    private SubmittedVariantMonotonicAccessioningService accessioningService;

    private DbsnpSubmittedVariantMonotonicAccessioningService accessioningServiceDbsnp;

    private Long accessioningMonotonicInitSs;

    private ContigAliasService contigAliasService;

    public SubmittedVariantAccessioningService(SubmittedVariantMonotonicAccessioningService accessioningService,
                                               DbsnpSubmittedVariantMonotonicAccessioningService accessioningServiceDbsnp,
                                               Long accessioningMonotonicInitSs,
                                               ContigAliasService contigAliasService) {
        this.accessioningService = accessioningService;
        this.accessioningServiceDbsnp = accessioningServiceDbsnp;
        this.accessioningMonotonicInitSs = accessioningMonotonicInitSs;
        this.contigAliasService = contigAliasService;
    }

    @Override
    public List<GetOrCreateAccessionWrapper<ISubmittedVariant, String, Long>> getOrCreate(
            List<? extends ISubmittedVariant> variants)
            throws AccessionCouldNotBeGeneratedException {
        List<AccessionWrapper<ISubmittedVariant, String, Long>> dbsnpVariants = accessioningServiceDbsnp.get(variants);
        List<ISubmittedVariant> variantsNotInDbsnp = removeFromList(variants, dbsnpVariants);
        if (variantsNotInDbsnp.isEmpty()) {
            // check this special case because mongo bulk inserts don't allow inserting empty lists
            // (accession-commons BasicMongoDbAccessionedCustomRepositoryImpl.insert would need to change)
            return dbsnpVariants.stream().map(d -> new GetOrCreateAccessionWrapper<>
                    (d.getAccession(),
                     d.getHash(),
                     d.getData(), false)).collect(Collectors.toList());
        } else {
            List<AccessionWrapper<ISubmittedVariant, String, Long>> submittedVariants = new ArrayList<>();
            accessioningService.getOrCreate(variantsNotInDbsnp)
                               .forEach(getOrCreateAccessionWrapperObj -> submittedVariants.add(
                                       new AccessionWrapper<ISubmittedVariant, String, Long>
                                               (getOrCreateAccessionWrapperObj.getAccession(),
                                                getOrCreateAccessionWrapperObj.getHash(),
                                                getOrCreateAccessionWrapperObj.getData())));
            return joinLists(submittedVariants, dbsnpVariants)
                    .stream()
                    .map(d -> new GetOrCreateAccessionWrapper<>
                            (d.getAccession(),
                             d.getHash(),
                             d.getData(), false)).collect(Collectors.toList());
        }
    }

    private List<ISubmittedVariant> removeFromList(List<? extends ISubmittedVariant> allVariants,
                                                   List<AccessionWrapper<ISubmittedVariant, String, Long>>
                                                           variantsToDelete) {
        Set<String> hashesToDelete = variantsToDelete.stream()
                                                     .map(AccessionWrapper::getHash)
                                                     .collect(Collectors.toSet());

        return allVariants.stream()
                          .map(variant -> Pair.of(accessioningServiceDbsnp.getHash(variant), variant))
                          .filter(pair -> !hashesToDelete.contains(pair.getFirst()))
                          .map(Pair::getSecond)
                          .collect(Collectors.toList());
    }

    private List<AccessionWrapper<ISubmittedVariant, String, Long>> joinLists(
            List<AccessionWrapper<ISubmittedVariant, String, Long>> l1,
            List<AccessionWrapper<ISubmittedVariant, String, Long>> l2) {
        l1.addAll(l2);
        return l1;
    }

    @Override
    public List<AccessionWrapper<ISubmittedVariant, String, Long>> get(List<? extends ISubmittedVariant> variants) {
        return joinLists(accessioningService.get(variants), accessioningServiceDbsnp.get(variants));
    }

    public List<AccessionWrapper<ISubmittedVariant, String, Long>> getAllByIdFields(
            String assembly, String contig, List<String> studies, long start, String reference, String alternate) {
        List<SubmittedVariant> submittedVariants = studies.stream()
                .map(study -> new SubmittedVariant(assembly, 0, study, contig, start, reference, alternate, null))
                .collect(Collectors.toList());
        List<AccessionWrapper<ISubmittedVariant, String, Long>> variants = this.get(submittedVariants);
        return variants;
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

    @Override
    public AccessionVersionsWrapper<ISubmittedVariant, String, Long> update(Long accession, int version,
                                                                            ISubmittedVariant iSubmittedVariant)
            throws AccessionDoesNotExistException, HashAlreadyExistsException, AccessionDeprecatedException,
            AccessionMergedException {
        if (accession >= accessioningMonotonicInitSs) {
            return accessioningService.update(accession, version, iSubmittedVariant);
        } else {
            return accessioningServiceDbsnp.update(accession, version, iSubmittedVariant);
        }
    }

    @Override
    public AccessionVersionsWrapper<ISubmittedVariant, String, Long> patch(Long accession, ISubmittedVariant variant)
            throws AccessionDoesNotExistException, HashAlreadyExistsException, AccessionDeprecatedException,
            AccessionMergedException {
        if (accession >= accessioningMonotonicInitSs) {
            return accessioningService.patch(accession, variant);
        } else {
            return accessioningServiceDbsnp.patch(accession, variant);
        }
    }

    @Override
    public void deprecate(Long accession, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        if (accession >= accessioningMonotonicInitSs) {
            accessioningService.deprecate(accession, reason);
        } else {
            accessioningServiceDbsnp.deprecate(accession, reason);
        }
    }

    @Override
    public void merge(Long accessionOrigin, Long mergeInto, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        if (accessionOrigin >= accessioningMonotonicInitSs && mergeInto >= accessioningMonotonicInitSs) {
            accessioningService.merge(accessionOrigin, mergeInto, reason);
        } else if (accessionOrigin < accessioningMonotonicInitSs && mergeInto < accessioningMonotonicInitSs) {
            accessioningServiceDbsnp.merge(accessionOrigin, mergeInto, reason);
        } else {
            throw new UnsupportedOperationException("Can't merge a submitted variant with a dbSNP submitted variant");
        }
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
