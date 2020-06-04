package com.serverless;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class StepDurationsTest {
    @Test
    /*
     *  Valid start date and end date
     * */
    public void StartnEndDateCase() {
        StepDurationsHandler handler = new StepDurationsHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("start", "2020-01-01");
        queryStringParameters.put("end", "2020-05-20");
        input.put("queryStringParameters", queryStringParameters);
        input.put("body",
                "{\n" +
                        "\t\"organizations\": [\"Org 2\"]\n" +
                        "}"
        );

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }
}
