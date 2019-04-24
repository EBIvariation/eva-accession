package uk.ac.ebi.eva.accession.ws.dto;

public class BeaconError {

    private int errorCode;

    private String errorMessage;

    public BeaconError(int errorCode, String errorMessage) {
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public int getErrorCode() {
        return errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
