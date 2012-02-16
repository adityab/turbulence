package com.turbulence.core.actions;

import java.net.URI;
import org.w3c.dom.Document;

public class ActionFactory {
    public static RegisterSchemaAction createRegisterSchemaAction(URI schemaURI) {
        return new RegisterSchemaAction(schemaURI);
    }

    public static StoreDataAction createStoreDataAction(Document data) {
        return new StoreDataAction(data);
    }
}
