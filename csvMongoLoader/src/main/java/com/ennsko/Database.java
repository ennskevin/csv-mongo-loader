package com.ennsko;

public class Database {
    public static final int localPort = 27017;
    public static final String connection = String.format("mongodb://localhost:%d", localPort);
}
