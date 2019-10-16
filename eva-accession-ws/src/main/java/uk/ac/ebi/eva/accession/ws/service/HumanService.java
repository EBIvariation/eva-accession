package uk.ac.ebi.eva.accession.ws.service;

import org.springframework.stereotype.Service;

import uk.ac.ebi.eva.accession.core.persistence.DbsnpClusteredVariantEntity;
import uk.ac.ebi.eva.accession.core.repositoryHuman.DbsnpHumanClusteredVariantAccessionRepository;

import java.util.List;

@Service
public class HumanService {

    private DbsnpHumanClusteredVariantAccessionRepository humanRepository;

    public HumanService(DbsnpHumanClusteredVariantAccessionRepository humanRepository) {
        this.humanRepository = humanRepository;
    }

    public List<DbsnpClusteredVariantEntity> getRs(Long id) {
        List<DbsnpClusteredVariantEntity> rsIds = humanRepository.findByAccession(id);
        return rsIds;
    }
}
