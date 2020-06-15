package com.serverless;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;


public class AllOrgsTest {
    @Test
    /*
     *  Valid start date and end date
     * */
    public void StartnEndDateCase() {
        AllOrgsHandler handler = new AllOrgsHandler();

        Map<String, Object> input = new HashMap<String, Object>();

        ApiGatewayResponse response = handler.handleRequest(input, null);
        System.out.println(response.getBody());
        assertEquals(response.getStatusCode(), 200);
    }
}
