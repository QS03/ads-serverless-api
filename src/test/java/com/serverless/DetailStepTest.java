package com.serverless;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class DetailStepTest {
    @Test
    /*
     *  Valid start date and end date
     * */
    public void StartnEndDateCase() {
        DetailStepHandler handler = new DetailStepHandler();

        Map<String, Object> input = new HashMap<String, Object>();
        Map<String, String> queryStringParameters = new HashMap<String, String>() ;
        queryStringParameters.put("start", "2020-01-01");
        queryStringParameters.put("end", "2020-05-20");
        queryStringParameters.put("casenumber", "Ticket 23");
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
