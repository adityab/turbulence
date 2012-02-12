package com.turbulence.core.actions;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement
public class Result {
    public boolean success;
    public int error;
    public String message;
}
