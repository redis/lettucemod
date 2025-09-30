package com.redis.lettucemod.utils;

import io.lettuce.core.SslOptions;

import java.io.File;

public class SslOptionsBuilder {

    private File keystore;

    private char[] keystorePassword;

    private File truststore;

    private char[] truststorePassword;

    private File keyCert;

    private File key;

    private char[] keyPassword;

    private File trustedCerts;

    public SslOptions.Builder build() {
        SslOptions.Builder ssl = SslOptions.builder();
        if (key != null) {
            ssl.keyManager(keyCert, key, keyPassword);
        }
        if (keystore != null) {
            ssl.keystore(keystore, keystorePassword);
        }
        if (truststore != null) {
            ssl.truststore(SslOptions.Resource.from(truststore), truststorePassword);
        }
        if (trustedCerts != null) {
            ssl.trustManager(trustedCerts);
        }
        return ssl;
    }

    public SslOptionsBuilder keystore(File keystore) {
        this.keystore = keystore;
        return this;
    }

    public char[] keystorePassword() {
        return keystorePassword;
    }

    public SslOptionsBuilder keystorePassword(char[] keystorePassword) {
        this.keystorePassword = keystorePassword;
        return this;
    }

    public File truststore() {
        return truststore;
    }

    public SslOptionsBuilder truststore(File truststore) {
        this.truststore = truststore;
        return this;
    }

    public char[] truststorePassword() {
        return truststorePassword;
    }

    public SslOptionsBuilder truststorePassword(char[] truststorePassword) {
        this.truststorePassword = truststorePassword;
        return this;
    }

    public File keyCert() {
        return keyCert;
    }

    public SslOptionsBuilder keyCert(File keyCert) {
        this.keyCert = keyCert;
        return this;
    }

    public File key() {
        return key;
    }

    public SslOptionsBuilder key(File key) {
        this.key = key;
        return this;
    }

    public char[] keyPassword() {
        return keyPassword;
    }

    public SslOptionsBuilder keyPassword(char[] keyPassword) {
        this.keyPassword = keyPassword;
        return this;
    }

    public File trustedCerts() {
        return trustedCerts;
    }

    public SslOptionsBuilder trustedCerts(File trustedCerts) {
        this.trustedCerts = trustedCerts;
        return this;
    }

}
