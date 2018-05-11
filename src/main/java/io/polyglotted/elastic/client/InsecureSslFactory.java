package io.polyglotted.elastic.client;

import lombok.AccessLevel;
import lombok.RequiredArgsConstructor;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.ssl.SSLContexts;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import java.security.KeyStore;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

import static java.util.Objects.requireNonNull;
import static javax.net.ssl.TrustManagerFactory.getDefaultAlgorithm;

@SuppressWarnings("WeakerAccess") @Slf4j
public abstract class InsecureSslFactory {

    @SneakyThrows static SSLContext insecureSslContext(String host, int port) {
        KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
        keyStore.load(null, new char[0]);

        SSLContext context = SSLContext.getInstance("TLS");
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(getDefaultAlgorithm());
        tmf.init(keyStore);

        X509TrustManager defaultTrustManager = (X509TrustManager) tmf.getTrustManagers()[0];
        CachingTrustManager tm = new CachingTrustManager(defaultTrustManager);
        context.init(null, new TrustManager[]{tm}, null);
        SSLSocketFactory factory = context.getSocketFactory();

        log.debug("Insecure SSL connection to " + host + ":" + port);
        SSLSocket socket = (SSLSocket) factory.createSocket(host, port);
        socket.setSoTimeout(10000);
        try {
            log.trace("Starting SSL handshake...");
            socket.startHandshake();
            socket.close();
            log.debug("No SSL errors, certificate is already trusted");
            return null;
        } catch (SSLException ignored) { }

        X509Certificate[] chain = requireNonNull(tm.chain, "Could not obtain server certificate chain");
        for (int k = 0; k < chain.length; k++) {
            keyStore.setCertificateEntry(host + "-" + (k + 1), chain[k]);
        }

        return SSLContexts.custom().loadTrustMaterial(keyStore, new TrustSelfSignedStrategy()).build();
    }

    @RequiredArgsConstructor(access = AccessLevel.PRIVATE)
    private static class CachingTrustManager implements X509TrustManager {
        private final X509TrustManager tm;
        private X509Certificate[] chain;

        @Override public X509Certificate[] getAcceptedIssuers() { return new X509Certificate[0]; }

        @Override public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            throw new UnsupportedOperationException();
        }

        public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
            this.chain = chain;
            tm.checkServerTrusted(chain, authType);
        }
    }
}