package com.serverless;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class ASAPInspectorTest {
    @Test
    /*
     *  Valid start date and end date
     * */
    public void ValidCase() {
        ASAPInspectorHandler handler = new ASAPInspectorHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        input.put("queryStringParameters", queryStringParameters);

        input.put("body",
                "{\n" +
                        "\t\"organizations\": [\"Org 1\"]\n" +
                "}"
        );

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());

        assertEquals(response.getStatusCode(), 200);
    }
}
