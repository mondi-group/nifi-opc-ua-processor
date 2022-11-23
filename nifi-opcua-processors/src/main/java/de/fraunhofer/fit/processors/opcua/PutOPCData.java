package de.fraunhofer.fit.processors.opcua;

import de.fraunhofer.fit.opcua.OPCUAService;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.BufferedReader;
import java.io.EOFException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A NiFi processor that writes the data received via a FlowFile to the selected OPC UA service.
 */
public class PutOPCData extends AbstractProcessor {

    // Attributes
    public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
            .name("OPC UA Service")
            .description("Specifies the OPC UA Service that can be used to access data")
            .required(true)
            .identifiesControllerService(OPCUAService.class)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor EXCLUDE_NULL_VALUE = new PropertyDescriptor
            .Builder().name("Exclude Null Value")
            .description("Writes data only for non null values")
            .required(true)
            .sensitive(false)
            .allowableValues("No", "Yes")
            .defaultValue("No")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor DELIMITER_CHARACTER = new PropertyDescriptor
            .Builder().name("Delimiter characters")
            .description("Characters used as delimiter between Node Id and Node Value")
            .required(true)
            .sensitive(false)
            .defaultValue(";")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor EOL_CHARACTER = new PropertyDescriptor
            .Builder().name("End of line characters")
            .description("Characters used as delimiter between lines of data (Node Id and Node Value)")
            .required(true)
            .sensitive(false)
            .defaultValue("\n")
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    // Relationships
    public static final Relationship SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();

    public static final Relationship FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OPCUA_SERVICE);
        // TODO consider if to really use this...
        // descriptors.add(EXCLUDE_NULL_VALUE);
        descriptors.add(DELIMITER_CHARACTER);
        descriptors.add(EOL_CHARACTER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(SUCCESS);
        relationships.add(FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return this.relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return descriptors;
    }

    @Override
    public void onTrigger(ProcessContext processContext, ProcessSession processSession) throws ProcessException {
        // Get the FlowFile
        FlowFile flowFile = processSession.get();
        try {
            // Init
            OPCUAService opcUAService;
            // If the FlowFile is null, nothing will happen
            if (flowFile != null){
                // Connect to the service
                opcUAService = processContext.getProperty(OPCUA_SERVICE).asControllerService(OPCUAService.class);
                // Read FlowFile content and send to OPC UA service to upsert data
                processSession.read(flowFile, in -> {
                    try {
                        // Read the content of the FlowFile and split by EndOfLine separator
                        String[] dataPoints = new BufferedReader(new InputStreamReader(in))
                                .lines()
                                .collect(Collectors.joining())
                                .split(processContext.getProperty(EOL_CHARACTER).toString());
                        // Extract all the key/value pairs
                        Map<String, String> keyValuePairs = new HashMap<String, String>();
                        for(String data : dataPoints){
                            String[] kv = data.split(processContext.getProperty(DELIMITER_CHARACTER).toString());
                            if(kv.length == 3)
                                keyValuePairs.put(kv[0] + ";" + kv[1], kv[2]); // Canonical form for Node Id is: 'ns=123;i=42'
                            else
                                throw new ProcessException("Exactly 3 values are required: namespace index, node identifier and node value, all separated by the chosen 'delimiter characters'. Found " + kv.length + " instead.");
                        }
                        // If parsing was successful, then continue with writing values to OPC UA service
                        opcUAService.putValues(keyValuePairs);
                    } catch (ProcessException e) {
                        // TODO ProcessException should be thrown only by the OPC UA Service, so we can use either yield or penalize...
                        getLogger().error("Failed to process FlowFile: " + e.getMessage());
                    }
                });
                // TODO Maybe we should cleanup the content of the FlowFile and just keep the attributes...
                // Notify the SEND event for a correct data lineage in the Provenance Repository
                processSession.getProvenanceReporter().send(flowFile, "SEND");
                // Transfer to SUCCESS relationship
                processSession.transfer(flowFile, SUCCESS);
            }
        } catch (Exception e) {
            getLogger().error("Failed to process FlowFile: " + e.getMessage());
            // Penalize only for data issues in the FlowFile, not for connection related issues, so that we can try again soon...
            processSession.penalize(flowFile);
            // TODO Yield should be used for Ingress not Egress. There is already an incoming FlowFile that will be routed to Failure already
            // processContext.yield();
            processSession.transfer(flowFile, FAILURE);
        }
    }

}
