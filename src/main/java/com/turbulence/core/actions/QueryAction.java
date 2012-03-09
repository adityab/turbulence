package com.turbulence.core.actions;

import org.apache.xerces.impl.xpath.regex.RegularExpression;

import java.util.*;

import com.hp.hpl.jena.graph.*;

import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.DatasetFactory;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.query.ResultSetFormatter;

import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.sparql.core.*;

import com.hp.hpl.jena.sparql.expr.Expr;
import com.hp.hpl.jena.sparql.syntax.*;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryFactory;

import com.turbulence.core.ClusterSpaceJenaGraph;

class Visitor extends ElementVisitorBase {
    @Override
    public void visit(ElementAssign el) {
        // TODO Auto-generated method stub
        System.err.println("assign");
    }

    @Override
    public void visit(ElementBind el) {
        // TODO Auto-generated method stub
        System.err.println("bind");
    }

    @Override
    public void visit(ElementDataset el) {
        // TODO Auto-generated method stub
        System.err.println("dataset");
    }

    @Override
    public void visit(ElementExists el) {
        // TODO Auto-generated method stub
        System.err.println("exists");
    }

    @Override
    public void visit(ElementFetch el) {
        // TODO Auto-generated method stub
        System.err.println("fetch");
    }

    @Override
    public void visit(ElementFilter el) {
        // TODO Auto-generated method stub
        System.err.println("filter");
        Expr exp = el.getExpr();
    }

    @Override
    public void visit(ElementMinus el) {
        // TODO Auto-generated method stub
        System.err.println("minus");
    }

    @Override
    public void visit(ElementNamedGraph el) {
        // TODO Auto-generated method stub
        System.err.println("named");
    }

    @Override
    public void visit(ElementNotExists el) {
        // TODO Auto-generated method stub
        System.err.println("notexists");
    }

    @Override
    public void visit(ElementOptional el) {
        // TODO Auto-generated method stub
        System.err.println("optional");
    }

    @Override
    public void visit(ElementPathBlock el) {
        // TODO Auto-generated method stub
        System.err.println("pathblock");
        Iterator<TriplePath> trips = el.patternElts();
        while (trips.hasNext()) {
            Triple t = trips.next().asTriple();
            //System.err.println(t.getSubject().getURI() + " " + t.getPredicate().getURI() + " " + t.getObject().getName());
            Node obj = t.getObject();
            if (obj.isURI()) {
                System.err.println("Object " + obj.getURI());
            }
            else if (obj.isVariable()) {
                System.err.println("Object " + obj.getName());
            }
            else if (obj.isLiteral()) {
                System.err.println("Object " + obj.getLiteralValue());
            }
        }
    }

    @Override
    public void visit(ElementService el) {
        // TODO Auto-generated method stub
        System.err.println("service");
    }

    @Override
    public void visit(ElementSubQuery el) {
        // TODO Auto-generated method stub
        System.err.println("subquery");
    }

    @Override
    public void visit(ElementUnion el) {
        // TODO Auto-generated method stub
        System.err.println("Union");
    }

    public void visit(ElementGroup g) {
        System.err.println("Group");
        for (Element el : g.getElements())
            el.visit(new Visitor());
    }

    public void visit(ElementTriplesBlock el) {
        System.err.println("Trip block");
        Iterator<Triple> trips = el.patternElts();
        while (trips.hasNext())
            System.err.println(trips.next().getSubject().getName());
    }
}

public class QueryAction implements Action {
    private final String query;
    /**
     * Constructs a new instance.
     */
    public QueryAction(String query)
    {
        this.query = query;
    }

	public Result perform() {
        Query q = QueryFactory.create(query);

        Model model = ModelFactory.createModelForGraph(new ClusterSpaceJenaGraph());
        QueryExecution exec = QueryExecutionFactory.create(q, model);
        ResultSet result = exec.execSelect();

        System.err.println("Result ---");
        System.err.println(ResultSetFormatter.asText(result));
        /*System.err.println("project variables");
        for (Var v : q.getProjectVars()) {
            System.err.println(v.getName());
        }

        Element qpat = q.getQueryPattern();
        qpat.visit(new Visitor());
        return null;*/
        return null;
	}
}
