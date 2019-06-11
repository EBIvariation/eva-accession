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
package uk.ac.ebi.eva.accession.core;

import uk.ac.ebi.ampt2d.commons.accession.core.AccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.BasicAccessioningService;
import uk.ac.ebi.ampt2d.commons.accession.core.DatabaseService;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionCouldNotBeGeneratedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDeprecatedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionDoesNotExistException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.AccessionMergedException;
import uk.ac.ebi.ampt2d.commons.accession.core.exceptions.HashAlreadyExistsException;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionVersionsWrapper;
import uk.ac.ebi.ampt2d.commons.accession.core.models.AccessionWrapper;
import uk.ac.ebi.ampt2d.commons.accession.hashing.SHA1HashingFunction;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpMonotonicAccessionGenerator;
import uk.ac.ebi.eva.accession.core.service.DbsnpClusteredVariantMonotonicAccessioningService;
import uk.ac.ebi.eva.accession.core.summary.ClusteredVariantSummaryFunction;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Service entry point for accessioning and querying clustered variants.
 * <p>
 * TODO Support EVA clustered variants, see {@link SubmittedVariantAccessioningService} for reference
 */
public class ClusteredVariantAccessioningService implements AccessioningService<IClusteredVariant, String, Long> {

    private DbsnpClusteredVariantMonotonicAccessioningService accessioningServiceDbsnp;

    public ClusteredVariantAccessioningService(
            DbsnpClusteredVariantMonotonicAccessioningService accessioningServiceDbsnp) {
        this.accessioningServiceDbsnp = accessioningServiceDbsnp;
    }

    public List<AccessionWrapper<IClusteredVariant, String, Long>> getByHashedMessageIn(List<String> hashes) {
        return hashes.stream().flatMap(hash -> accessioningServiceDbsnp.getByHash(Collections.singletonList(hash))
                                                                       .stream()).collect(Collectors.toList());
    }

    @Override
    public List<AccessionWrapper<IClusteredVariant, String, Long>> getOrCreate(List<? extends IClusteredVariant>
                                                                                       variants)
            throws AccessionCouldNotBeGeneratedException {
        return accessioningServiceDbsnp.getOrCreate(variants);
    }

    @Override
    public List<AccessionWrapper<IClusteredVariant, String, Long>> get(List<? extends IClusteredVariant> variants) {
        return accessioningServiceDbsnp.get(variants);
    }

    @Override
    public AccessionWrapper<IClusteredVariant, String, Long> getByAccession(Long accession)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        return accessioningServiceDbsnp.getByAccession(accession);
    }

    @Override
    public AccessionWrapper<IClusteredVariant, String, Long> getByAccessionAndVersion(Long accession, int version)
            throws AccessionMergedException, AccessionDoesNotExistException, AccessionDeprecatedException {
        return accessioningServiceDbsnp.getByAccessionAndVersion(accession, version);
    }

    @Override
    public AccessionVersionsWrapper<IClusteredVariant, String, Long> update(Long accession, int version,
                                                                            IClusteredVariant iClusteredVariant)
            throws AccessionDeprecatedException, AccessionDoesNotExistException, AccessionMergedException,
            HashAlreadyExistsException {
        return accessioningServiceDbsnp.update(accession, version, iClusteredVariant);
    }

    @Override
    public AccessionVersionsWrapper<IClusteredVariant, String, Long> patch(Long accession,
                                                                           IClusteredVariant iClusteredVariant)
            throws AccessionDeprecatedException, AccessionDoesNotExistException, AccessionMergedException,
            HashAlreadyExistsException {
        return accessioningServiceDbsnp.patch(accession, iClusteredVariant);
    }

    @Override
    public void deprecate(Long accession, String reason) throws AccessionMergedException,
            AccessionDoesNotExistException, AccessionDeprecatedException {
        accessioningServiceDbsnp.deprecate(accession, reason);
    }

    @Override
    public void merge(Long accessionOrigin, Long mergeInto, String reason) throws AccessionMergedException,
            AccessionDoesNotExistException, AccessionDeprecatedException {
        accessioningServiceDbsnp.merge(accessionOrigin, mergeInto, reason);
    }
}