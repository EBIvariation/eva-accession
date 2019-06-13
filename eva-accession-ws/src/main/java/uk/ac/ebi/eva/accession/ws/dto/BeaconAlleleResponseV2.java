package uk.ac.ebi.eva.accession.ws.dto;

import uk.ac.ebi.eva.commons.beacon.models.BeaconAlleleRequestBody;
import uk.ac.ebi.eva.commons.beacon.models.DatasetAlleleResponse;

import java.util.List;

public class BeaconAlleleResponseV2 {

    private String beaconId;

    private String apiVersion;

    private Boolean exists;

    private BeaconAlleleRequestBody alleleRequest;

    private BeaconError error;

    private List<DatasetAlleleResponse> datasetAlleleResponses;

    public BeaconAlleleResponseV2() { }

    public BeaconAlleleResponseV2(String beaconId, String apiVersion, Boolean exists,
                                      BeaconAlleleRequestBody alleleRequest, BeaconError error,
                                      List<DatasetAlleleResponse> datasetAlleleResponses) {
        this.beaconId = beaconId;
        this.apiVersion = apiVersion;
        this.exists = exists;
        this.alleleRequest = alleleRequest;
        this.error = error;
        this.datasetAlleleResponses = datasetAlleleResponses;
    }

    public String getBeaconId() {
        return beaconId;
    }

    public void setBeaconId(String beaconId) {
        this.beaconId = beaconId;
    }

    public String getApiVersion() {
        return apiVersion;
    }

    public void setApiVersion(String apiVersion) {
        this.apiVersion = apiVersion;
    }

    public Boolean getExists() {
        return exists;
    }

    public void setExists(Boolean exists) {
        this.exists = exists;
    }

    public BeaconAlleleRequestBody getAlleleRequest() {
        return alleleRequest;
    }

    public void setAlleleRequest(BeaconAlleleRequestBody alleleRequest) {
        this.alleleRequest = alleleRequest;
    }

    public BeaconError getError() {
        return error;
    }

    public void setError(BeaconError error) {
        this.error = error;
    }

    public List<DatasetAlleleResponse> getDatasetAlleleResponses() {
        return datasetAlleleResponses;
    }

    public void setDatasetAlleleResponses(List<DatasetAlleleResponse> datasetAlleleResponses) {
        this.datasetAlleleResponses = datasetAlleleResponses;
    }

}
