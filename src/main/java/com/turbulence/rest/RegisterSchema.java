package com.turbulence.rest;

import java.net.URI;
import javax.ws.rs.*;

@Path("/api/1/register_schema")
public class RegisterSchema {
    @GET
    @Produces("application/xml")
    public String registerSchemaURL(@QueryParam("url") URI url) {
        return url.toString();
    }

    @POST
    @Consumes("application/rdf+xml")
    @Produces("application/xml")
    public String registerSchemaBody(String body) {
        System.err.println(body);
        return "";
    }
}
