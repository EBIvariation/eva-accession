/*
 *
 * Copyright 2020 EMBL - European Bioinformatics Institute
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

import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasNaming;
import uk.ac.ebi.eva.accession.core.contigalias.ContigAliasService;
import uk.ac.ebi.eva.accession.core.model.ClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.IClusteredVariant;
import uk.ac.ebi.eva.accession.core.model.ISubmittedVariant;
import uk.ac.ebi.eva.accession.core.service.nonhuman.dbsnp.DbsnpClusteredVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.service.nonhuman.eva.ClusteredVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.commons.core.models.VariantType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class ClusteredVariantAccessioningService implements AccessioningService<IClusteredVariant, String, Long> {

    private static Logger logger = LoggerFactory.getLogger(ClusteredVariantAccessioningService.class);

    private ClusteredVariantMonotonicAccessioningService accessioningService;

    private DbsnpClusteredVariantMonotonicAccessioningService accessioningServiceDbsnp;

    private Long accessioningMonotonicInitRs;

    private final ContigAliasService contigAliasService;

    public ClusteredVariantAccessioningService(ClusteredVariantMonotonicAccessioningService accessioningService,
                                               DbsnpClusteredVariantMonotonicAccessioningService accessioningServiceDbsnp,
                                               Long accessioningMonotonicInitRs,
                                               ContigAliasService contigAliasService) {
        this.accessioningService = accessioningService;
        this.accessioningServiceDbsnp = accessioningServiceDbsnp;
        this.accessioningMonotonicInitRs = accessioningMonotonicInitRs;
        this.contigAliasService = contigAliasService;
    }

    @Override
    public List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> getOrCreate(
            List<? extends IClusteredVariant> variants)
            throws AccessionCouldNotBeGeneratedException {
        List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> dbsnpVariants =
                accessioningServiceDbsnp.get(variants).stream()
                        .map(d -> new GetOrCreateAccessionWrapper<>(d.getAccession(), d.getHash(), d.getData(), false))
                        .collect(Collectors.toList());
        List<IClusteredVariant> variantsNotInDbsnp = removeFromList(variants, dbsnpVariants);
        if (variantsNotInDbsnp.isEmpty()) {
            // check this special case because mongo bulk inserts don't allow inserting empty lists
            // (accession-commons BasicMongoDbAccessionedCustomRepositoryImpl.insert would need to change)
            return dbsnpVariants;
        } else {
            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> clusteredVariants =
                    accessioningService.getOrCreate(variantsNotInDbsnp);

            List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>> allClusteredVariants = new ArrayList<>();
            allClusteredVariants.addAll(dbsnpVariants);
            allClusteredVariants.addAll(clusteredVariants);

            return allClusteredVariants;
        }
    }

    private List<IClusteredVariant> removeFromList(List<? extends IClusteredVariant> allVariants,
                                                   List<GetOrCreateAccessionWrapper<IClusteredVariant, String, Long>>
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

    private List<AccessionWrapper<IClusteredVariant, String, Long>> joinLists(
            List<AccessionWrapper<IClusteredVariant, String, Long>> l1,
            List<AccessionWrapper<IClusteredVariant, String, Long>> l2) {
        l1.addAll(l2);
        return l1;
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
            Long accession, ContigAliasNaming contigAliasNaming)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        List<AccessionWrapper<IClusteredVariant, String, Long>> clusteredVariants;
        if (accession >= accessioningMonotonicInitRs) {
            clusteredVariants = accessioningService.getAllByAccession(accession);
        } else {
            clusteredVariants = accessioningServiceDbsnp.getAllByAccession(accession);
        }
        return contigAliasService.getClusteredVariantsWithTranslatedContig(clusteredVariants, contigAliasNaming);
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>>
    getAllActiveByAssemblyAndAccessionIn(String assembly, List<Long> accessionList) {
        List<Long> evaAccessions = new ArrayList<>();
        List<Long> dbsnpAccessions = new ArrayList<>();
        for (Long accession: accessionList) {
            if (accession >= accessioningMonotonicInitRs) {
                evaAccessions.add(accession);
            } else {
                dbsnpAccessions.add(accession);
            }
        }

        List<AccessionWrapper<IClusteredVariant, String, Long>> result =
                accessioningService.getAllActiveByAssemblyAndAccessionIn(assembly, evaAccessions);
        result.addAll(accessioningServiceDbsnp.getAllActiveByAssemblyAndAccessionIn(assembly, dbsnpAccessions));
        return result;
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

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getByIdFields(
            String assembly, String contig, long start, VariantType type) {
        IClusteredVariant clusteredVariant = new ClusteredVariant(assembly, 0, contig, start, type, false, null);
        List<AccessionWrapper<IClusteredVariant, String, Long>> variants = this.get(
                Collections.singletonList(clusteredVariant));
        return variants;
    }

    @Override
    public AccessionVersionsWrapper<IClusteredVariant, String, Long> update(Long accession, int version,
                                                                            IClusteredVariant iClusteredVariant)
            throws AccessionDoesNotExistException, HashAlreadyExistsException, AccessionDeprecatedException,
            AccessionMergedException {
        if (accession >= accessioningMonotonicInitRs) {
            return accessioningService.update(accession, version, iClusteredVariant);
        } else {
            return accessioningServiceDbsnp.update(accession, version, iClusteredVariant);
        }
    }

    @Override
    public AccessionVersionsWrapper<IClusteredVariant, String, Long> patch(Long accession, IClusteredVariant variant)
            throws AccessionDoesNotExistException, HashAlreadyExistsException, AccessionDeprecatedException,
            AccessionMergedException {
        if (accession >= accessioningMonotonicInitRs) {
            return accessioningService.patch(accession, variant);
        } else {
            return accessioningServiceDbsnp.patch(accession, variant);
        }
    }

    @Override
    public void deprecate(Long accession, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        if (accession >= accessioningMonotonicInitRs) {
            accessioningService.deprecate(accession, reason);
        } else {
            accessioningServiceDbsnp.deprecate(accession, reason);
        }
    }

    @Override
    public void merge(Long accessionOrigin, Long mergeInto, String reason)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        if (accessionOrigin >= accessioningMonotonicInitRs && mergeInto >= accessioningMonotonicInitRs) {
            accessioningService.merge(accessionOrigin, mergeInto, reason);
        } else if (accessionOrigin < accessioningMonotonicInitRs && mergeInto < accessioningMonotonicInitRs) {
            accessioningServiceDbsnp.merge(accessionOrigin, mergeInto, reason);
        } else {
            throw new UnsupportedOperationException("Can't merge a clustered variant with a dbSNP clustered variant");
        }
    }

    public AccessionWrapper<IClusteredVariant, String, Long> getLastInactive(Long accession) {
        if (accession >= accessioningMonotonicInitRs) {
            return accessioningService.getLastInactive(accession);
        } else {
            return accessioningServiceDbsnp.getLastInactive(accession);
        }
    }

}
