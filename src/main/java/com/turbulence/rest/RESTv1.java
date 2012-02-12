package com.turbulence.rest;

import java.net.URI;
import javax.ws.rs.*;

import com.turbulence.core.actions.*;

@Path("/api/1")
public class RESTv1 {
    @GET("/register_schema")
    @Produces("application/xml")
    public String registerSchemaURL(@QueryParam("url") URI url) {
        return url.toString();
    }

    @POST("/register_schema")
    @Consumes("application/rdf+xml")
    @Produces("application/xml")
    public String registerSchemaBody(String body) {
        // TODO how it *SHOULD* work
        Future<Result> res = ActionFactory
        return res.get();
    }
}
