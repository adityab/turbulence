package com.turbulence.rest;

import java.io.InputStream;

import java.net.URI;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import javax.ws.rs.*;

import javax.ws.rs.core.StreamingOutput;

import com.turbulence.core.actions.*;
import com.turbulence.core.TurbulenceDriver;

@Path("/api/1")
public class RESTv1 {

    private Result execute(final Action action) {
        try {
            Future<Result> ret = TurbulenceDriver.submit(new Callable<Result>() {
                public Result call() {
                    return action.perform();
                }
            });
            return ret.get();
        } catch (ExecutionException e) {
            Result r = new Result();
            r.error = TurbulenceError.UNKNOWN_ERROR;
            r.message = e.getMessage();
            e.printStackTrace();
            return r;
        } catch (InterruptedException e) {
            Result r = new Result();
            r.error = TurbulenceError.THREAD_INTERRUPTED;
            r.message = e.getMessage();
            e.printStackTrace();
            return r;
        }
    }

    @POST
    @Path("/register_schema")
    @Consumes("application/x-www-form-urlencoded")
    @Produces("application/xml")
    public Result registerSchemaURL(@FormParam("url") URI url) {
        final RegisterSchemaAction act = ActionFactory.createRegisterSchemaAction(url);
        return execute(act);
    }

    @POST
    @Path("/store_data")
    @Consumes("application/xml")
    @Produces("application/xml")
    public Result storeData(final InputStream in) {
        final StoreDataAction act = ActionFactory.createStoreDataAction(in);
        return execute(act);
    }

    @POST
    @Path("/store_xml_data")
    @Consumes("application/xml")
    @Produces("application/xml")
    public Result storeXMLData(final InputStream in) {
        final StoreXMLDataAction act = ActionFactory.createStoreXMLDataAction(in);
        return execute(act);
    }

    @POST
    @Path("/query")
    @Produces("application/xml")
    public StreamingOutput query(String body) {
        System.err.println("QUERY:" + body);
        final QueryAction act = ActionFactory.createQueryAction(body);
        return act.stream();
    }

    @POST
    @Path("/lubm_query")
    @Produces("application/xml")
    public Result lubmQuery(String body) {
        final LUBMQueryAction act = ActionFactory.createLUBMQueryAction(body);
        return execute(act);
    }
}
