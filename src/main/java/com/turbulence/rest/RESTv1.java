package com.turbulence.rest;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.*;

import com.turbulence.core.actions.*;
import com.turbulence.core.TurbulenceDriver;

@Path("/api/1")
public class RESTv1 {
    @POST
    @Path("/register_schema")
    @Consumes("application/x-www-form-urlencoded")
    @Produces("application/xml")
    public Result registerSchemaURL(@FormParam("url") URI url) {
        final RegisterSchemaAction act = ActionFactory.createRegisterSchemaAction(url);
        try {
            Future<Result> ret = TurbulenceDriver.submit(new Callable<Result>() {
                public Result call() {
                    return act.perform();
                }
            });
            return ret.get();
        } catch (ExecutionException e) {
            Result r = new Result();
            r.error = 45;
            r.message = e.getMessage();
            return r;
        } catch (InterruptedException e) {
            Result r = new Result();
            r.error = 46;
            r.message = e.getMessage();
            return r;
        }
    }
}
