/*
 * Neue Vesion Milo 1.1.1
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.mondigroup.nifi_opcua_bundle;

import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.ValidationResult;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.eclipse.milo.opcua.sdk.client.AddressSpace;
import org.eclipse.milo.opcua.sdk.client.DiscoveryClient;
import org.eclipse.milo.opcua.sdk.client.OpcUaClient;
import org.eclipse.milo.opcua.sdk.client.OpcUaClientConfigBuilder;
import org.eclipse.milo.opcua.sdk.client.identity.AnonymousProvider;
import org.eclipse.milo.opcua.sdk.client.identity.IdentityProvider;
import org.eclipse.milo.opcua.sdk.client.identity.UsernameProvider;
import org.eclipse.milo.opcua.sdk.client.subscriptions.MonitoredItemSynchronizationException;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaMonitoredItem;
import org.eclipse.milo.opcua.sdk.client.subscriptions.OpcUaSubscription;
import org.eclipse.milo.opcua.stack.core.AttributeId;
import org.eclipse.milo.opcua.stack.core.Identifiers;
import org.eclipse.milo.opcua.stack.core.UaException;
import org.eclipse.milo.opcua.stack.core.security.*;
import org.eclipse.milo.opcua.stack.core.types.builtin.*;
import org.eclipse.milo.opcua.stack.core.types.enumerated.DataChangeTrigger;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MessageSecurityMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.MonitoringMode;
import org.eclipse.milo.opcua.stack.core.types.enumerated.TimestampsToReturn;
import org.eclipse.milo.opcua.stack.core.types.structured.DataChangeFilter;
import org.eclipse.milo.opcua.stack.core.types.structured.EndpointDescription;
import org.eclipse.milo.opcua.stack.core.types.structured.ReadValueId;
import org.eclipse.milo.opcua.stack.core.types.structured.ReferenceDescription;
import org.eclipse.milo.opcua.stack.core.util.CertificateUtil;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

@Tags({"opc"})
@CapabilityDescription("ControllerService implementation of OPCUAService.")
public class StandardOPCUAService extends AbstractControllerService implements OPCUAService {

    private static final Validator DIRECTORY_READ_WRITE_VALIDATOR = (subject, input, context) -> {
        if (input == null || input.trim().isEmpty()) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Directory path must not be empty.")
                    .build();
        }

        try {
            final Path p = Paths.get(input.trim());

            // Existiert bereits?
            if (Files.exists(p)) {
                if (!Files.isDirectory(p)) {
                    return new ValidationResult.Builder()
                            .subject(subject).input(input).valid(false)
                            .explanation("Path exists but is not a directory: " + p)
                            .build();
                }
                if (!Files.isReadable(p) || !Files.isWritable(p)) {
                    return new ValidationResult.Builder()
                            .subject(subject).input(input).valid(false)
                            .explanation("Directory must be readable and writable by the NiFi service user: " + p)
                            .build();
                }
                return new ValidationResult.Builder()
                        .subject(subject).input(input).valid(true).build();
            }

            // Existiert nicht: prüfen ob anlegbar
            final Path parent = p.getParent();
            if (parent == null) {
                return new ValidationResult.Builder()
                        .subject(subject).input(input).valid(false)
                        .explanation("Directory does not exist and parent directory is undefined: " + p)
                        .build();
            }

            if (!Files.exists(parent)) {
                return new ValidationResult.Builder()
                        .subject(subject).input(input).valid(false)
                        .explanation("Directory does not exist and parent directory does not exist: " + parent)
                        .build();
            }

            if (!Files.isDirectory(parent) || !Files.isWritable(parent)) {
                return new ValidationResult.Builder()
                        .subject(subject).input(input).valid(false)
                        .explanation("Directory does not exist and parent directory is not writable: " + parent)
                        .build();
            }

            // Parent ist writebar -> anlegbar -> valid
            return new ValidationResult.Builder()
                    .subject(subject).input(input).valid(true).build();

        } catch (Exception e) {
            return new ValidationResult.Builder()
                    .subject(subject)
                    .input(input)
                    .valid(false)
                    .explanation("Invalid directory path: " + e.getMessage())
                    .build();
        }
    };

    public static final PropertyDescriptor ENDPOINT = new PropertyDescriptor.Builder().name("Endpoint URL")
            .description("The opc.tcp address of the opc ua server, e.g. opc.tcp://192.168.0.2:48010")
            .required(true).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor SECURITY_POLICY = new PropertyDescriptor.Builder().name("Security Policy")
            .description("What security policy to use for connection with OPC UA server")
            .required(true).allowableValues("None", "Basic128Rsa15", "Basic256", "Basic256Sha256", "Aes256_Sha256_RsaPss", "Aes128_Sha256_RsaOaep")
            .defaultValue("None").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor SECURITY_MODE = new PropertyDescriptor.Builder().name("Security Mode")
            .description("What security mode to use for connection with OPC UA server. Only valid when \"Security Policy\" isn't \"None\".")
            .required(true).allowableValues("Sign", "SignAndEncrypt")
            .defaultValue("Sign").addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor APPLICATION_URI = new PropertyDescriptor.Builder().name("Application URI")
            .description("The application URI of your OPC-UA client. It must match the \"URI\" field in \"Subject Alternative Name\" of your client certificate. Typically it has the form of \"urn:aaa:bbb\". However, whether this field is checked depends on the implementation of the server. That means, for some servers, it is not necessary to specify this field.")
            .addValidator(Validator.VALID).build();

    public static final PropertyDescriptor CLIENT_KS_LOCATION = new PropertyDescriptor.Builder().name("Client Keystore Location").description("The location of the client keystore. Only valid when \"Security Policy\" isn't \"None\". " + "The keystore should contain only one keypair entry (private key + certificate). " + "If multiple entries exist, the first one is used. " + "Besides, the key should have the same password as the keystore.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor CLIENT_KS_PASSWORD = new PropertyDescriptor.Builder().name("Client Keystore Password").description("The password for the client keystore")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true).build();

    public static final PropertyDescriptor REQUIRE_SERVER_AUTH = new PropertyDescriptor.Builder().name("Require server authentication")
            .description("Whether to authenticate server by verifying its certificate against the trust store. It is recommended to disable this option for quick test, but enable it for production.")
            .allowableValues("true", "false")
            .defaultValue("false").addValidator(StandardValidators.BOOLEAN_VALIDATOR).build();

    public static final PropertyDescriptor TRUSTSTORE_LOCATION = new PropertyDescriptor.Builder().name("Trust store Location")
            .description("The location of the trust store. Only valid when \"Security Policy\" isn't \"None\". " + "Trust store contains trusted certificates, which are to be used for server identity verification." + "The trust store can contain multiple certificates.")
            .addValidator(StandardValidators.FILE_EXISTS_VALIDATOR)
            .build();

    public static final PropertyDescriptor TRUSTSTORE_PASSWORD = new PropertyDescriptor.Builder().name("Trust store Password")
            .description("The password for the trust store")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .sensitive(true).build();

    public static final PropertyDescriptor PKI_BASE_DIR = new PropertyDescriptor.Builder().name("PKI Base Directory")
            .description("Base directory for OPC UA PKI trust list storage (trusted/issuer/crl/rejected). The directory must be readable and writable by the NiFi service user. Use a persistent path (not target/...). Supports Expression Language via Variable Registry/Environment.")
            .required(true)
            .addValidator(DIRECTORY_READ_WRITE_VALIDATOR)
            .build();

    public static final PropertyDescriptor AUTH_POLICY = new PropertyDescriptor.Builder().name("Authentication Policy")
            .description("How should Nifi authenticate with the UA server").required(true)
            .defaultValue("Anon").allowableValues("Anon", "Username")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR).build();

    public static final PropertyDescriptor USERNAME = new PropertyDescriptor.Builder().name("User Name")
            .description("The user name to access the OPC UA server (only valid when \"Authentication Policy\" is \"Username\")")
            .required(false).addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PASSWORD = new PropertyDescriptor.Builder().name("Password")
            .description("The password to access the OPC UA server (only valid when \"Authentication Policy\" is \"Username\")")
            .required(false).sensitive(true).addValidator(Validator.VALID)
            .build();

    public static final PropertyDescriptor USE_PROXY = new PropertyDescriptor.Builder().name("Use Proxy")
            .description("If true, the \"Endpoint URL\" specified above will be used to establish connection to the server instead of the discovered URL. " + "Useful when connecting to OPC UA server behind NAT or through SSH tunnel, in which the discovered URL is not reachable by the client.")
            .required(true).defaultValue("false")
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .build();


    private static final List<PropertyDescriptor> properties;

    private OpcUaClient opcClient;
    private final Map<String, SubscriptionConfig> subscriptionMap = new ConcurrentHashMap<>();
    private final AtomicLong droppedMessages = new AtomicLong();
    private final AtomicBoolean shuttingDown = new AtomicBoolean(false);
    private volatile ExecutorService recreateExecutor;
    private static final long OPCUA_NULL_TS_UNIX_MILLIS = -11644473600000L;

    static {
        final List<PropertyDescriptor> props = new ArrayList<>();
        props.add(ENDPOINT);
        props.add(SECURITY_POLICY);
        props.add(SECURITY_MODE);
        props.add(APPLICATION_URI);
        props.add(CLIENT_KS_LOCATION);
        props.add(CLIENT_KS_PASSWORD);
        props.add(REQUIRE_SERVER_AUTH);
        props.add(TRUSTSTORE_LOCATION);
        props.add(TRUSTSTORE_PASSWORD);
        props.add(PKI_BASE_DIR);
        props.add(AUTH_POLICY);
        props.add(USERNAME);
        props.add(PASSWORD);
        props.add(USE_PROXY);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        // wichtig: nach einem Disable wieder freigeben
        shuttingDown.set(false);
        ensureRecreateExecutor();

        String endpoint = context.getProperty(ENDPOINT).getValue();
        if (endpoint == null) {
            throw new InitializationException("Endpoint can't be null.");
        }

        // Security Mode bestimmen
        MessageSecurityMode minSecurityMode;
        if (context.getProperty(SECURITY_POLICY).getValue().equals("None")) {
            minSecurityMode = MessageSecurityMode.None;
        } else if (context.getProperty(SECURITY_MODE).getValue().equals("Sign")) {
            minSecurityMode = MessageSecurityMode.Sign;
        } else {
            minSecurityMode = MessageSecurityMode.SignAndEncrypt;
        }

        /// Security Policy bestimmen
        SecurityPolicy minSecurityPolicy;
        switch (context.getProperty(SECURITY_POLICY).getValue()) {
            case "Basic128Rsa15":
                minSecurityPolicy = SecurityPolicy.Basic128Rsa15;
                break;
            case "Basic256":
                minSecurityPolicy = SecurityPolicy.Basic256;
                break;
            case "Basic256Sha256":
                minSecurityPolicy = SecurityPolicy.Basic256Sha256;
                break;
            case "Aes256_Sha256_RsaPss":
                minSecurityPolicy = SecurityPolicy.Aes256_Sha256_RsaPss;
                break;
            case "Aes128_Sha256_RsaOaep":
                minSecurityPolicy = SecurityPolicy.Aes128_Sha256_RsaOaep;
                break;
            default:
                minSecurityPolicy = SecurityPolicy.None;
                minSecurityMode = MessageSecurityMode.None;
                break;
        }

        try {

            // Endpoint discovery mit Timeout (wichtig!)
            List<EndpointDescription> endpoints =
                    DiscoveryClient.getEndpoints(endpoint).get(50000, TimeUnit.SECONDS);

            EndpointDescription endpointDescription = chooseEndpoint(endpoints, minSecurityPolicy, minSecurityMode);

            if (endpointDescription == null) {
                StringBuilder sb = new StringBuilder();

                sb.append(String.format(
                        "No exact security configuration match is found.%n" +
                                "You specified security mode: %s, security policy: %s%n" +
                                "Available combinations:%n",
                        minSecurityMode.name(), minSecurityPolicy.getUri()
                ));

                for (EndpointDescription ed : endpoints) {
                    sb.append(String.format("security mode: %s, security policy: %s%n",
                            ed.getSecurityMode().name(), ed.getSecurityPolicyUri()));
                }
                throw new InitializationException(sb.toString());
            }

            OpcUaClientConfigBuilder cfgBuilder = new OpcUaClientConfigBuilder();
            // global request timeout (nicht nur im Security-Zweig)
            cfgBuilder.setRequestTimeout(uint(10000));

            // The following code is used to force the client to connect to the URL given by user,
            // instead of using the discovered URL. Useful when client is visiting the server through
            // some NAT or SSH tunneling, and the discovered URL is not reachable.
            if (context.getProperty(USE_PROXY).asBoolean()) {
                endpointDescription = new EndpointDescription(
                        endpoint,
                        endpointDescription.getServer(),
                        endpointDescription.getServerCertificate(),
                        endpointDescription.getSecurityMode(),
                        endpointDescription.getSecurityPolicyUri(),
                        endpointDescription.getUserIdentityTokens(),
                        endpointDescription.getTransportProfileUri(),
                        endpointDescription.getSecurityLevel());
            }

            cfgBuilder.setEndpoint(endpointDescription);

            if (!minSecurityPolicy.equals(SecurityPolicy.None)) {  // If security policy is used

                final String baseDirRaw = context.getProperty(PKI_BASE_DIR).getValue();

                final Path pkiBaseDir = Paths.get(baseDirRaw);
                Files.createDirectories(pkiBaseDir);

                TrustListManager clientTrustListManager = FileBasedTrustListManager.createAndInitialize(pkiBaseDir);
                List<X509Certificate> serverCerts = CertificateUtil.decodeCertificates(endpointDescription.getServerCertificate().bytes());
                if (!serverCerts.isEmpty()) {
                    clientTrustListManager.addTrustedCertificate(serverCerts.get(0));
                }

                DefaultClientCertificateValidator certificateValidator =
                        new DefaultClientCertificateValidator(clientTrustListManager, new MemoryCertificateQuarantine());
                cfgBuilder.setCertificateValidator(certificateValidator);

                // clientKsLocation has already been validated, no need to check again
                String clientKsLocation = context.getProperty(CLIENT_KS_LOCATION).getValue();
                char[] clientKsPassword = context.getProperty(CLIENT_KS_PASSWORD).getValue() != null ? context.getProperty(CLIENT_KS_PASSWORD).getValue().toCharArray() : null;

                // Verify server certificate against the trust store
                if (context.getProperty(REQUIRE_SERVER_AUTH).asBoolean()) {

                    // trustStoreLocation has already been validated, no need to check again
                    String trustStoreLocation = context.getProperty(TRUSTSTORE_LOCATION).getValue();
                    char[] trustStorePassword = context.getProperty(TRUSTSTORE_PASSWORD).getValue() != null ? context.getProperty(TRUSTSTORE_PASSWORD).getValue().toCharArray() : null;

                    TrustStoreLoader tsLoader = new TrustStoreLoader().load(trustStoreLocation, trustStorePassword);
                    //List<X509Certificate> serverCerts = CertificateUtil.decodeCertificates(endpointDescription.getServerCertificate().bytes());

                    try {
                        // Only verify the first certificate, and CA certificate at the end of the chain. Intermediate certificates are not verified
                        tsLoader.verify(serverCerts);
                    } catch (Exception e) {
                        getLogger().error("Cannot verify server certificate. Please ensure server cert is in trust store.", e);
                        throw new InitializationException(e.getMessage());
                    }
                }

                KeyStoreLoader loader = new KeyStoreLoader().load(clientKsLocation, clientKsPassword);
                cfgBuilder.setCertificate(loader.getClientCertificate());
                cfgBuilder.setKeyPair(loader.getClientKeyPair());
                cfgBuilder.setCertificateChain(loader.getClientCertificateChain());


                //X509Certificate c = loader.getClientCertificate();
                //getLogger().info("CLIENT Subject={}", c.getSubjectX500Principal());
                //getLogger().info("CLIENT EKU={}", c.getExtendedKeyUsage());
                //getLogger().info("CLIENT KeyUsage={}", java.util.Arrays.toString(c.getKeyUsage()));
                //getLogger().info("CLIENT SAN={}", c.getSubjectAlternativeNames());

            }

            // Identity Provider setzen
            String authType = context.getProperty(AUTH_POLICY).getValue();
            IdentityProvider identityProvider;
            if (authType.equals("Anon")) {
                identityProvider = new AnonymousProvider();
            } else {
                String username = context.getProperty(USERNAME).getValue();
                String password = context.getProperty(PASSWORD).getValue();
                identityProvider = new UsernameProvider(username == null ? "" : username, password == null ? "" : password);
            }

            cfgBuilder.setIdentityProvider(identityProvider);

            String applicationUri = context.getProperty(APPLICATION_URI).getValue();
            if (applicationUri != null) {
                cfgBuilder.setApplicationUri(applicationUri);
                cfgBuilder.setProductUri(applicationUri);
            }

            // Client erzeugen + Connect Timeout über connectAsync()
            opcClient = OpcUaClient.create(cfgBuilder.build());
            opcClient.connectAsync().get(30, TimeUnit.SECONDS);

        } catch (TimeoutException te) {
            safeCloseClient();
            throw new InitializationException("OPC UA connect timeout after 10 seconds", te);
        } catch (ExecutionException ee) {
            safeCloseClient();
            throw new InitializationException("OPC UA connect failed: " + ee.getCause(), ee);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            safeCloseClient();
            throw new InitializationException("OPC UA connect interrupted", ie);
        } catch (InitializationException iex) {
            safeCloseClient();
            throw iex;
        } catch (Exception e) {
            safeCloseClient();
            throw new InitializationException("OPC UA initialization failed", e);
        }
    }

    private void safeCloseClient() {
        final OpcUaClient client = this.opcClient;
        this.opcClient = null;
        if (client != null) {
            try {
                client.disconnectAsync().get(3, TimeUnit.SECONDS);
            } catch (Exception ignore) {
            }
        }
    }

    @OnDisabled
    public void shutdown() {
        shuttingDown.set(true);

        // 1) Recreate-Executor stoppen
        final ExecutorService ex = recreateExecutor;
        recreateExecutor = null;

        if (ex != null) {
            try {
                ex.shutdown();
                if (!ex.awaitTermination(5, TimeUnit.SECONDS)) {
                    ex.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                ex.shutdownNow();
            } catch (Exception e) {
                getLogger().warn("Error while shutting down recreateExecutor: {}", e.getMessage(), e);
            }
        }

        // 2) Subscriptions best-effort löschen und Map leeren
        try {
            if (subscriptionMap != null && !subscriptionMap.isEmpty()) {
                // Snapshot, damit wir nicht ConcurrentModification bekommen
                final List<Map.Entry<String, SubscriptionConfig>> entries =
                        new ArrayList<>(subscriptionMap.entrySet());

                for (Map.Entry<String, SubscriptionConfig> entry : entries) {
                    final String subId = entry.getKey();
                    final SubscriptionConfig cfg = entry.getValue();

                    if (cfg == null) continue;

                    try {
                        // Milo 1.x: Subscription am Objekt selbst löschen
                        cfg.getSubscription().delete();
                    } catch (Exception e) {
                        getLogger().debug("Failed to delete subscription {} (ignored): {}", subId, e.getMessage());
                    }
                }

                subscriptionMap.clear();
            }
        } catch (Exception e) {
            getLogger().warn("Error while cleaning subscriptions: {}", e.getMessage(), e);
        }

        // 3) OPC UA Client disconnect
        final OpcUaClient client = this.opcClient;
        this.opcClient = null;


        if (client != null) {
            try {
                client.disconnectAsync().get(5, TimeUnit.SECONDS);
            } catch (TimeoutException te) {
                // Timeout ist nicht ideal, aber im NiFi Disable willst du nicht hängen bleiben
                getLogger().warn("OPC UA disconnect timeout (ignored): {}", te.getMessage());
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                getLogger().warn("OPC UA disconnect interrupted (ignored): {}", ie.getMessage());
            } catch (ExecutionException ee) {
                getLogger().warn("OPC UA disconnect failed (ignored): {}", ee.getCause() != null ? ee.getCause().getMessage() : ee.getMessage());
            } catch (Exception e) {
                getLogger().warn("OPC UA disconnect failed (ignored): {}", e.getMessage());
            }
        }
    }


    @Override
    public byte[] getValue(List<String> tagNames, String returnTimestamp, boolean excludeNullValue, String nullValueString) throws ProcessException {

        try {
            if (shuttingDown.get()) {
                throw new ProcessException("Service is shutting down");
            }

            if (opcClient == null) {
                throw new ProcessException("OPC Client is null. OPC UA service was not enabled properly.");
            }

            // TODO: Throw more descriptive exception when parsing fails
            ArrayList<NodeId> nodeIdList = new ArrayList<>();
            tagNames.forEach((tagName) -> nodeIdList.add(NodeId.parse(tagName)));

            List<DataValue> rvList = opcClient.readValues(0, TimestampsToReturn.Both, nodeIdList);

            StringBuilder serverResponse = new StringBuilder();

            for (int i = 0; i < tagNames.size(); i++) {
                String valueLine;
                valueLine = writeCsv(tagNames.get(i), returnTimestamp, rvList.get(i), excludeNullValue, nullValueString);
                serverResponse.append(valueLine);
            }

            return serverResponse.toString().trim().getBytes();

        } catch (Exception e) {
            throw new ProcessException(e);
        }

    }

    @Override
    public String subscribe(List<String> tagNames, BlockingQueue<String> queue, boolean tsChangedNotify, long minPublishInterval) throws ProcessException {

        try {

            if (shuttingDown.get()) {
                throw new ProcessException("Service is shutting down");
            }

            if (opcClient == null) {
                throw new Exception("OPC Client is null. OPC UA service was not enabled properly.");
            }

            List<ReadValueId> readValueIds = new ArrayList<>();
            tagNames.forEach((tagName) -> {
                ReadValueId readValueId = new ReadValueId(NodeId.parse(tagName), AttributeId.Value.uid(), null, QualifiedName.NULL_VALUE);
                readValueIds.add(readValueId);
            });

            DataChangeFilter changeFilter = tsChangedNotify ? new DataChangeFilter(DataChangeTrigger.from(2), null, null) : null;

            OpcUaSubscription sub = createSubscription();

            createMonitorItems(sub, readValueIds, queue, changeFilter);

            return putSubToMap(sub, queue, readValueIds, changeFilter);

        } catch (Exception e) {
            throw new ProcessException(e.getMessage());
        }
    }

    private void recreateSubscription(String oldSubId, OpcUaSubscription oldSub, SubscriptionConfig cfg) {
        try {

            // Config aus Map entfernen, damit externe unsubscribe nicht kollidiert
            subscriptionMap.remove(oldSubId);
            try {
                oldSub.delete();
            } catch (Exception ignore) {
            }


            OpcUaSubscription newSub = createSubscription();
            createMonitorItems(newSub, cfg.getReadValueIds(), cfg.getQueue(), cfg.getDataChangeFilter());
            putSubToMap(newSub, cfg.getQueue(), cfg.getReadValueIds(), cfg.getDataChangeFilter());

            getLogger().info("Recreated subscription oldSubId={} newSubId={}", oldSubId, newSub.getSubscriptionId());

        } catch (Exception e) {
            getLogger().error("Recreating subscription failed!", e);
        }
    }

    @Override
    public void unsubscribe(String subscriptionUid) {

        if (opcClient == null) {
            getLogger().warn("OPC Client is null. OPC UA service was not enabled properly.");
            return;
        }

        SubscriptionConfig cfg = subscriptionMap.remove(subscriptionUid);
        if (cfg == null) return;

        try {
            cfg.getSubscription().delete();
        } catch (Exception e) {
            getLogger().warn("Unsubscribe failed (delete subscription): {}", e.getMessage());
        }


    }

    private ExecutorService ensureRecreateExecutor() {
        ExecutorService ex = recreateExecutor;
        if (ex == null || ex.isShutdown() || ex.isTerminated()) {
            synchronized (this) {
                ex = recreateExecutor;
                if (ex == null || ex.isShutdown() || ex.isTerminated()) {
                    recreateExecutor = ex = Executors.newSingleThreadExecutor(new ThreadFactory() {
                        private final AtomicInteger n = new AtomicInteger(1);

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread t = new Thread(r, "opcua-recreate-" + n.getAndIncrement());
                            t.setDaemon(true);
                            return t;
                        }
                    });
                }
            }
        }
        return ex;
    }

    @Override
    public void putValue(String namespace, String variable, Object value, Boolean withTimestamp) throws ProcessException {
        final ComponentLog logger = getLogger();

        if (opcClient == null) {
            logger.error("OPC Client is null. OPC UA service was not enabled properly.");
            throw new ProcessException("OPC Client is null.");
        }

        try {
            String node = namespace + VALUE_SEPARATOR + variable;
            NodeId nodeId = NodeId.parse(node);

            Variant v = new Variant(value);

            // Build DataValue (with or without timestamp)
            final DataValue data;
            if (Boolean.TRUE.equals(withTimestamp)) {
                // sourceTimestamp setzen (üblich beim Schreiben; serverTimestamp wird meist vom Server gesetzt)
                data = new DataValue(v, StatusCode.GOOD, DateTime.now(), null);
            } else {
                data = new DataValue(v, StatusCode.GOOD, null, null);
            }

            CompletableFuture<List<StatusCode>> future =
                    opcClient.writeValuesAsync(List.of(nodeId), List.of(data));

            List<StatusCode> results = future.get();
            StatusCode sc = results.get(0);

            if (sc.isBad()) {
                throw new ProcessException("Failed to write " + value + " for node: " + node + ". Status " + sc);
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ProcessException("Interrupted while writing OPC UA value.", e);

        } catch (ExecutionException e) {
            Throwable cause = e.getCause() != null ? e.getCause() : e;
            throw new ProcessException("OPC UA write failed: " + cause.getMessage(), cause);

        } catch (Exception e) {
            throw new ProcessException(e);
        }
    }

    @Override
    public byte[] getNodes(String indentString, int maxRecursiveDepth, int maxReferencePerNode, boolean printNonLeafNode, String rootNodeId) throws ProcessException {

        try {
            if (opcClient == null) {
                throw new ProcessException("OPC Client is null. OPC UA service was not enabled properly.");
            }

            NodeId nodeId;
            if (rootNodeId == null || rootNodeId.isEmpty()) {
                nodeId = Identifiers.RootFolder;
            } else {
                nodeId = NodeId.parse(rootNodeId);
            }

            StringBuilder builder = new StringBuilder();
            browseNodeIteratively("", indentString, maxRecursiveDepth, maxReferencePerNode, printNonLeafNode, opcClient, nodeId, builder);

            return builder.toString().getBytes();

        } catch (Exception e) {
            throw new ProcessException(e.getMessage());
        }

    }

    // Choose the proper endpoint from discovered endpoints according to security settings
    private EndpointDescription chooseEndpoint(List<EndpointDescription> endpoints, SecurityPolicy minSecurityPolicy, MessageSecurityMode minMessageSecurityMode) {

        for (EndpointDescription endpoint : endpoints) {
            SecurityPolicy endpointSecurityPolicy;
            try {
                endpointSecurityPolicy = SecurityPolicy.fromUri(endpoint.getSecurityPolicyUri());
            } catch (UaException e) {
                continue;
            }
            if (minSecurityPolicy.compareTo(endpointSecurityPolicy) == 0 && minMessageSecurityMode.compareTo(endpoint.getSecurityMode()) == 0) {
                // Found endpoint which fulfills minimum requirements
                return endpoint;
            }
        }
        return null;
    }

    private void browseNodeIteratively(
            String currentIndent,
            String indentString,
            int remainDepth,
            int maxRefPerNode,
            boolean printNonLeafNode,
            OpcUaClient client,
            NodeId browseRoot,
            StringBuilder builder) {

        try {
            AddressSpace.BrowseOptions browseOptions = AddressSpace.BrowseOptions.builder()
                    .setMaxReferencesPerNode(uint(maxRefPerNode))
                    .build();

            List<ReferenceDescription> refs =
                    client.getAddressSpace()
                            .browseAsync(browseRoot, browseOptions)
                            .orTimeout(10, TimeUnit.SECONDS)
                            .join();

            boolean hasChildren = refs != null && !refs.isEmpty();

            if (printNonLeafNode || !hasChildren) {
                builder.append(currentIndent)
                        .append(getFullName(browseRoot))
                        .append("\n");
            }

            if (!hasChildren || remainDepth <= 0) {
                return;
            }

            String newIndent = currentIndent + indentString;
            int nextDepth = remainDepth - 1;

            int count = 0;
            for (ReferenceDescription rd : refs) {
                if (count++ >= maxRefPerNode) break;

                NodeId childId = rd.getNodeId()
                        .toNodeId(client.getNamespaceTable())
                        .orElse(null);

                if (childId != null) {
                    browseNodeIteratively(
                            newIndent,
                            indentString,
                            nextDepth,
                            maxRefPerNode,
                            printNonLeafNode,
                            client,
                            childId,
                            builder
                    );
                }
            }

        } catch (CompletionException ce) {
            Throwable cause = ce.getCause() != null ? ce.getCause() : ce;
            getLogger().warn("Browsing nodeId={} failed: {}", browseRoot, cause.getMessage());
        }
    }


    private String getFullName(NodeId nodeId) {

        String identifierType;

        switch (nodeId.getType()) {
            case Numeric:
                identifierType = "i";
                break;
            case Opaque:
                identifierType = "b";
                break;
            case Guid:
                identifierType = "g";
                break;
            default:
                identifierType = "s";
        }

        return String.format("ns=%s;%s=%s", nodeId.getNamespaceIndex().toString(), identifierType, nodeId.getIdentifier().toString());
    }


    private OpcUaSubscription createSubscription() throws Exception {

        final OpcUaSubscription sub = new OpcUaSubscription(opcClient);

        // Listener pro Subscription setzen (statt Manager.addSubscriptionListener)
        sub.setSubscriptionListener(new CustomSubscriptionListener());

        // Subscription am Server anlegen
        sub.create();

        return sub;
    }


    private void createMonitorItems(OpcUaSubscription uaSubscription, List<ReadValueId> readValueIds, BlockingQueue<String> queue, DataChangeFilter df) throws Exception {

        for (ReadValueId rvid : readValueIds) {

            OpcUaMonitoredItem mi = new OpcUaMonitoredItem(rvid, MonitoringMode.Reporting);

            mi.setSamplingInterval(300.0);
            mi.setQueueSize(uint(10));
            mi.setDiscardOldest(true);
            if (df != null) {
                mi.setFilter(df);
            }

            mi.setDataValueListener((item, value) -> {
                getLogger().debug("subscription value received: item=" + item.getReadValueId().getNodeId() + " value=" + value.getValue());

                String valueLine = writeCsv(getFullName(item.getReadValueId().getNodeId()), "Both", value, false, "");

                boolean ok = queue.offer(valueLine);
                if (!ok) {
                    long dropped = droppedMessages.incrementAndGet();
                    if (dropped % 1000 == 0) {
                        getLogger().warn("Subscription queue is full. Dropped {} messages so far.", dropped);
                    }
                }

            });

            // Item zur Subscription hinzufügen
            uaSubscription.addMonitoredItem(mi);
        }


        try {
            uaSubscription.synchronizeMonitoredItems();
        } catch (MonitoredItemSynchronizationException e) {
            e.getCreateResults().forEach(result -> {
                getLogger().warn("failed to create item: nodeId=" + result.monitoredItem().getReadValueId().getNodeId() + ", serviceResult=" + result.serviceResult() + ", operationResult=" + result.operationResult());
            });

            throw e;
        }

    }

    // Put SubscriptionConfig to a map for later retrieval
    private String putSubToMap(OpcUaSubscription sub, BlockingQueue<String> queue, List<ReadValueId> readValueIds, DataChangeFilter dataChangeFilter) {
        String subUid = sub.getSubscriptionId().toString();
        subscriptionMap.put(subUid, new SubscriptionConfig(sub, queue, readValueIds, dataChangeFilter));
        return subUid;
    }


    private String writeCsv(String tagName, String returnTimestamp, DataValue value, boolean excludeNullValue, String nullValueString) {

        String sValue = nullValueString;

        if (value == null || value.getValue() == null || value.getValue().getValue() == null) {

            if (excludeNullValue) {
                getLogger().debug("Null value returned for " + tagName + " -- Skipping because property is set");
                return "";
            }

        } else {

            // Check the type of variant
            if (value.getValue().getValue().getClass().isArray()) {

                StringBuilder sb = new StringBuilder();
                Object[] arr = (Object[]) value.getValue().getValue();
                for (Object o : arr) {
                    sb.append(o.toString()).append(";");
                }
                sValue = sb.toString();

            } else {
                sValue = value.getValue().getValue().toString();
            }

        }

        StringBuilder valueLine = new StringBuilder();

        valueLine.append(tagName).append(",");

        if (("ServerTimestamp").equals(returnTimestamp) || ("Both").equals(returnTimestamp)) {
            if (value.getServerTime() != null) valueLine.append(value.getServerTime().getJavaTime());
            valueLine.append(",");
        }
        if (("SourceTimestamp").equals(returnTimestamp) || ("Both").equals(returnTimestamp)) {
            Long ts = null;

            if (value.getSourceTime() != null) {
                long candidate = value.getSourceTime().getJavaTime();
                // 1601-01-01 (oder alles davor) als "nicht gesetzt" behandeln
                if (candidate > OPCUA_NULL_TS_UNIX_MILLIS) {
                    ts = candidate;
                }
            }

            // fallback: use server time if source time not set
            if (ts == null) {
                ts = value.getServerTime().getJavaTime();
            }

            if (ts != null) {
                valueLine.append(ts);
            }

            valueLine.append(",");
        }

        valueLine.append(sValue);
        valueLine.append(",");

        valueLine.append(value.getStatusCode().getValue()).append(System.getProperty("line.separator"));

        return valueLine.toString();
    }


    // Special class as container to wrap subscription with the queue connected to a SubscribeOPCUANodes processor
    private static class SubscriptionConfig {
        private final OpcUaSubscription subscription;
        private final BlockingQueue<String> queue;
        private final List<ReadValueId> readValueIds;
        private final DataChangeFilter dataChangeFilter;

        SubscriptionConfig(OpcUaSubscription subscription, BlockingQueue<String> queue, List<ReadValueId> readValueIds, DataChangeFilter df) {
            this.subscription = subscription;
            this.queue = queue;
            this.readValueIds = readValueIds;
            this.dataChangeFilter = df;
        }

        public OpcUaSubscription getSubscription() {
            return subscription;
        }

        BlockingQueue<String> getQueue() {
            return queue;
        }

        List<ReadValueId> getReadValueIds() {
            return readValueIds;
        }

        DataChangeFilter getDataChangeFilter() {
            return dataChangeFilter;
        }
    }

    // Custom SubscriptionListener to handle recreating subscription when transfer fails
    private class CustomSubscriptionListener implements OpcUaSubscription.SubscriptionListener {

        public void onPublishFailure(UaException exception) {
            getLogger().warn("Subscription publish failure: {} status={}", exception.getMessage(), exception.getStatusCode());
        }


        @Override
        public void onTransferFailed(OpcUaSubscription subscription, StatusCode statusCode) {

            final String oldSubId = subscription.getSubscriptionId().toString();
            if (shuttingDown.get()) {
                getLogger().debug("Service is shutting down; skip recreate for subId={}", oldSubId);
                return;
            }

            getLogger().warn("Subscription transfer failed: {} for subId={} -> recreating...", statusCode, oldSubId);

            final SubscriptionConfig cfg = subscriptionMap.get(oldSubId);
            if (cfg == null) {
                getLogger().warn("No subscription config found for subscriptionId={}, cannot recreate.", oldSubId);
                return;
            }

            // Nicht im Milo-Callback-Thread blockieren -> async
            try {
                ensureRecreateExecutor().submit(() -> recreateSubscription(oldSubId, subscription, cfg));
            } catch (RejectedExecutionException ree) {
                getLogger().warn("Recreate executor is shut down; skipping recreate for subId={}", oldSubId);
            }

        }
    }


}
