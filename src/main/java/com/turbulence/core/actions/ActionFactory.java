package com.turbulence.core.actions;

import java.net.URI;

public class ActionFactory {
    public static RegisterSchemaAction createRegisterSchemaAction(URI schemaURI) {
        return new RegisterSchemaAction(schemaURI);
    }
}
