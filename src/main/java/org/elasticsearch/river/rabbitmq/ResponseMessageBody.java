package org.elasticsearch.river.rabbitmq;

import org.json.JSONObject;

/**
 * {
 * success: true | false,
 * bulk_request: {},
 * bulkResponseFailureMessage: {},
 * fault_info: {}
 * }
 */
public class ResponseMessageBody {

    private boolean success;

    // although this is expected to be JSON, we pass as String, since it is possible it is unparsable JSON.
    private String bulkRequest;

    private FaultInfo faultInfo;

    public String toJson() {
        return new JSONObject(this).toString();
    }

    public void setSuccess(boolean success) {
        this.success = success;
    }

    public void setBulkRequest(String bulkRequest) {
        this.bulkRequest = bulkRequest;
    }

    public void setFaultInfo(FaultInfo faultInfo) {
        this.faultInfo = faultInfo;
    }

    public boolean isSuccess() {
        return success;
    }

    public String getBulkRequest() {
        return bulkRequest;
    }

    public FaultInfo getFaultInfo() {
        return faultInfo;
    }

}
