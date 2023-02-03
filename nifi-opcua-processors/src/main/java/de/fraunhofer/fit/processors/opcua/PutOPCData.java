package de.fraunhofer.fit.processors.opcua;

import de.fraunhofer.fit.opcua.OPCUAService;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.exception.*;
import org.apache.nifi.processor.util.StandardValidators;

import java.io.*;
// import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A NiFi processor that writes the data received via a FlowFile to the selected OPC UA service.
 */
@Tags({"opc"})
@CapabilityDescription("Upsert the data of specified nodes on the OPC UA server.")
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

    // Relationships
    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("Success")
            .description("Successful OPC read")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("Failure")
            .description("Failed OPC read")
            .build();

    private List<PropertyDescriptor> descriptors;
    private Set<Relationship> relationships;

    final static private String EOL_SEPARATOR = "\n";

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OPCUA_SERVICE);
        // TODO consider if to really use this...
        // descriptors.add(EXCLUDE_NULL_VALUE);
        descriptors.add(DELIMITER_CHARACTER);
        this.descriptors = Collections.unmodifiableList(descriptors);

        final Set<Relationship> relationships = new HashSet<Relationship>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
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

    @OnScheduled
    public void onScheduled(final ProcessContext context) {
        // TODO probably not needed...
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        // Get the logger
        final ComponentLog logger = getLogger();
        // Get the incoming FlowFile
        FlowFile flowFile = session.get();

        if (flowFile != null){
            // Init
            OPCUAService opcUAService = null;

            try {
                // Connect to the service
                opcUAService = context.getProperty(OPCUA_SERVICE).asControllerService(OPCUAService.class);
                if (opcUAService == null){
                    throw new NullPointerException("Null value found.");
                }
                logger.info("OPC UA service available.");
            }
            catch (Exception e){
                opcUAService = null;
                logger.error("OPC UA service not available: {}", e);
                session.transfer(flowFile, REL_FAILURE);
                // Avoid insisting with this processor, there is no connectivity to the outside service yet.
                context.yield();
            }

            if (opcUAService != null) {
                final String SEP = opcUAService.VALUE_SEPARATOR;
                List<String> values = new Vector<String>();
                // Clone the flowfile to capture the response
                //FlowFile responseFlowFile = session.create(flowFile);
                //session.getProvenanceReporter().clone(flowFile, responseFlowFile);

                try {
                    // Read FlowFile content and send to OPC UA service to upsert data
                    // If the callback fails, it will be handled by the surrounding try-catch statement
                    session.read(flowFile, in -> {
                        try(final InputStreamReader isr = new InputStreamReader(in)){
                            // Read the content of the FlowFile and split by EndOfLine separator
                            List<String> lines = new BufferedReader(isr).lines().collect(Collectors.toList());
                            // Extract all the key/value pairs
                            for(String line : lines){
                                String[] tokens = line.replace("\r", "")
                                        .split(context.getProperty(DELIMITER_CHARACTER).toString());
                                if(tokens.length == 3)
                                    values.add(tokens[0] + SEP + tokens[1] + SEP + tokens[2]); // Canonical form for Node Id is: 'ns=123;i=42;value'
                                else
                                    throw new RuntimeException("Exactly 3 values are required: namespace index, node identifier and node value, all separated by the chosen 'delimiter characters'. Found " + tokens.length + " in: '" + line + "'.");
                            }
                        }
                    });
                }
                catch (ProcessException pe){
                    logger.error("Error handling data: {}", pe);
                    //session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                }

                try {
                    // Write values to OPC UA service
                    opcUAService.putValues(values);
                    session.getProvenanceReporter().send(flowFile, context.getProperty(OPCUA_SERVICE).getValue());
                    // Commit the cloned FlowFile
                    session.transfer(flowFile, REL_SUCCESS);
                }
                catch (Exception e){
                    logger.error("Error writing values to OPC UA: {}", e);
                    // Penalize FlowFile to maybe retry. It could be a network issue only
                    //session.penalize(flowFile);
                    session.transfer(flowFile, REL_FAILURE);
                    context.yield();
                }
            }
        }

    }

}
