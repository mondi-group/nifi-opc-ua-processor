package com.mondigroup.nifi_opcua_bundle;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileInputStream;
import java.security.*;
import java.security.cert.X509Certificate;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.regex.Pattern;

class KeyStoreLoader {

    private static final Pattern IP_ADDR_PATTERN = Pattern.compile(
            "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$");

    private static String detectStoreType(String path) {
        String p = path.toLowerCase();
        if (p.endsWith(".p12") || p.endsWith(".pfx")) return "PKCS12";
        if (p.endsWith(".jks")) return "JKS";
        // Default: PKCS12 ist heutzutage am gängigsten
        return "PKCS12";
    }

    private final Logger logger = LoggerFactory.getLogger(getClass());

    private X509Certificate clientCertificate;
    private KeyPair clientKeyPair;
    private X509Certificate[] clientChain;

    KeyStoreLoader load(String ksLocation, char[] password) throws Exception {

        KeyStore keyStore = KeyStore.getInstance(detectStoreType(ksLocation));
        keyStore.load(new FileInputStream(ksLocation), password);

        clientCertificate = null;
        clientKeyPair = null;
        clientChain = null;

        Enumeration<String> aliases = keyStore.aliases();
        // Find the first PrivateKeyEntry in the keystore, which has both PrivateKey and Certificate
        while (aliases.hasMoreElements()) {
            String alias = aliases.nextElement();
            // Key and keystore share the same password
            Key clientPrivateKey = keyStore.getKey(alias, password);
            if (clientPrivateKey != null) {
                if (clientPrivateKey instanceof PrivateKey) {
                    if (keyStore.getCertificate(alias) != null) {
                        clientCertificate = (X509Certificate) keyStore.getCertificate(alias);
                        PublicKey clientPublicKey = clientCertificate.getPublicKey();
                        clientKeyPair = new KeyPair(clientPublicKey, (PrivateKey) clientPrivateKey);
                        clientChain = Arrays.stream(keyStore.getCertificateChain(alias))
                                .map(c -> (X509Certificate) c)
                                .toArray(X509Certificate[]::new);
                        break;
                    }
                }
            }
        }

        if (clientCertificate == null || clientKeyPair == null) {
            throw new Exception("No keypair found in keystore " + ksLocation);
        }

        return this;
    }

    X509Certificate getClientCertificate() {
        return clientCertificate;
    }

    KeyPair getClientKeyPair() {
        return clientKeyPair;
    }

    X509Certificate[] getClientCertificateChain() { return clientChain; }
}
