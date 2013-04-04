package org.elasticsearch.river.rabbitmq;

import org.testng.annotations.Test;

import java.io.IOException;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.testng.Assert.assertNotNull;

public class ResponseMessageBodyTest {

    @Test
    public void testResponseMessageBodyToJson() {
        ResponseMessageBody responseMessageBody = new ResponseMessageBody();
        responseMessageBody.setFaultInfo(new FaultInfo(new IOException("something nasty!")));
        System.out.println(responseMessageBody.toJson());
        // this is a bit dodgy cos order of elements in map is not guaranteed..
        assertThat(responseMessageBody.toJson(), equalTo("{\"faultInfo\":{\"errorMessage\":\"something nasty!\",\"errorName\":\"java.io.IOException\"},\"bulkRequest\":null,\"success\":false}"));
    }

}
