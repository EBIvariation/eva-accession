package uk.ac.ebi.eva.accession.ws.dto;

import uk.ac.ebi.ampt2d.commons.accession.rest.dto.AccessionResponseDTO;
import uk.ac.ebi.ampt2d.commons.accession.rest.dto.HistoryEventDTO;

import java.util.List;

public class VariantHistory<DTO, MODEL, HASH, ACCESSION> {

    private List<AccessionResponseDTO<DTO, MODEL, HASH, ACCESSION>> variants;
    private List<HistoryEventDTO<ACCESSION, DTO>> operations;

    public VariantHistory(){

    }

    public VariantHistory(List<AccessionResponseDTO<DTO, MODEL, HASH, ACCESSION>> variants,
                          List<HistoryEventDTO<ACCESSION, DTO>> operations) {
        this.variants = variants;
        this.operations = operations;
    }

    public List<AccessionResponseDTO<DTO, MODEL, HASH, ACCESSION>> getVariants() {
        return variants;
    }

    public List<HistoryEventDTO<ACCESSION, DTO>> getOperations() {
        return operations;
    }

    public void setVariants(List<AccessionResponseDTO<DTO, MODEL, HASH, ACCESSION>> variants) {
        this.variants = variants;
    }

    public void setOperations(List<HistoryEventDTO<ACCESSION, DTO>> operations) {
        this.operations = operations;
    }
}
