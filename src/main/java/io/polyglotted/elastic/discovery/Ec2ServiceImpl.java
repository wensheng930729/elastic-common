package io.polyglotted.elastic.discovery;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.retry.RetryPolicy;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import lombok.extern.slf4j.Slf4j;
import org.elasticsearch.common.Randomness;
import org.elasticsearch.common.settings.SecureString;
import org.elasticsearch.common.settings.Settings;

import java.util.Random;

@Slf4j
abstract class Ec2ServiceImpl implements Ec2Service {

    static AmazonEC2 client(Settings settings) {
        AmazonEC2ClientBuilder builder = AmazonEC2Client.builder().withCredentials(buildCredentials(settings))
            .withClientConfiguration(buildConfiguration(settings));
        if (REGION_SETTING.exists(settings)) {
            String endpoint = findEndpoint(settings);
            if (endpoint != null) {
                builder.setEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, REGION_SETTING.get(settings)));
            }
            else { builder.withRegion(Regions.fromName(REGION_SETTING.get(settings))); }
        }
        else { builder.withRegion(Regions.EU_WEST_1); }
        return builder.build();
    }

    private static AWSCredentialsProvider buildCredentials(Settings settings) {
        AWSCredentialsProvider credentials;
        try (SecureString key = ACCESS_KEY_SETTING.get(settings);
             SecureString secret = SECRET_KEY_SETTING.get(settings)) {
            if (key.length() == 0 && secret.length() == 0) {
                log.debug("Using either environment variables, system properties or instance profile credentials");
                credentials = new DefaultAWSCredentialsProviderChain();
            }
            else {
                log.debug("Using basic key/secret credentials");
                credentials = new AWSStaticCredentialsProvider(new BasicAWSCredentials(key.toString(), secret.toString()));
            }
        }
        return credentials;
    }

    private static ClientConfiguration buildConfiguration(Settings settings) {
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setResponseMetadataCacheSize(0);
        clientConfiguration.setProtocol(PROTOCOL_SETTING.get(settings));

        if (PROXY_HOST_SETTING.exists(settings)) {
            String proxyHost = PROXY_HOST_SETTING.get(settings);
            Integer proxyPort = PROXY_PORT_SETTING.get(settings);
            try (SecureString proxyUsername = PROXY_USERNAME_SETTING.get(settings);
                 SecureString proxyPassword = PROXY_PASSWORD_SETTING.get(settings)) {
                clientConfiguration.withProxyHost(proxyHost).withProxyPort(proxyPort)
                    .withProxyUsername(proxyUsername.toString()).withProxyPassword(proxyPassword.toString());
            }
        }
        final Random rand = Randomness.get();
        RetryPolicy retryPolicy = new RetryPolicy(
            RetryPolicy.RetryCondition.NO_RETRY_CONDITION,
            (originalRequest, exception, retriesAttempted) -> {
                log.warn("EC2 API request failed, retry again. Reason was:", exception);
                return 1000L * (long) (10d * Math.pow(2, retriesAttempted / 2.0d) * (1.0d + rand.nextDouble()));
            },
            10,
            false);
        clientConfiguration.setRetryPolicy(retryPolicy);
        clientConfiguration.setSocketTimeout((int) READ_TIMEOUT_SETTING.get(settings).millis());
        return clientConfiguration;
    }

    private static String findEndpoint(Settings settings) {
        String endpoint = null;
        if (ENDPOINT_SETTING.exists(settings)) {
            endpoint = ENDPOINT_SETTING.get(settings); log.debug("using explicit ec2 endpoint [{}]", endpoint);
        }
        return endpoint;
    }
}