package org.elasticsearch.river.rabbitmq;

public class FaultInfo {

    private String errorName;
    private String errorMessage;

    public FaultInfo(Throwable throwable) {
        errorName = throwable.getClass().getName();
        errorMessage = throwable.getMessage();
    }

    public FaultInfo(String errorName, String errorMessage) {
        this.errorName = errorName;
        this.errorMessage = errorMessage;
    }

    public String getErrorName() {
        return errorName;
    }

    public void setErrorName(String errorName) {
        this.errorName = errorName;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }
}
