package com.turbulence.core.actions;

import java.util.logging.Logger;

import java.net.URI;
import java.net.URISyntaxException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.Queue;
import java.util.LinkedList;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.*;
import org.semanticweb.owlapi.reasoner.OWLReasoner;
import org.semanticweb.owlapi.reasoner.NodeSet;

import org.w3c.dom.*;

import com.clarkparsia.pellet.owlapiv3.*;

import com.turbulence.core.*;
import com.turbulence.util.*;

public class StoreDataAction implements Action {
    private Logger logger;
    protected StoreDataAction(Document data) {
        logger = Logger.getLogger(this.getClass().getName());
        for (int i = 0; i < data.getChildNodes().getLength(); i++)
            recurse(data.getChildNodes().item(i), "");
    }

    public void recurse(Node n, String indent) {
        System.err.println(indent + "nsURI: " + n.getNamespaceURI());
        System.err.println(indent + "nodeName: " + n.getNodeName());
        NodeList l = n.getChildNodes();
        for (int i = 0; i < l.getLength(); i++) {
            recurse(l.item(i), indent + "  ");
        }
    }

    public Result perform() {
        return new Result();
    }
}
