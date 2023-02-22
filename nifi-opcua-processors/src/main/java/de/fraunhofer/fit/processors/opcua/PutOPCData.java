package de.fraunhofer.fit.processors.opcua;

import de.fraunhofer.fit.opcua.OPCUAService;
import de.fraunhofer.fit.processors.opcua.utils.BuiltInDataType;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.Validator;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.exception.*;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.Tuple;
import org.jooq.lambda.tuple.Tuple3;

import java.io.*;
// import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * A NiFi processor that writes the data received via a FlowFile to the selected OPC UA service.
 */
@Tags({"opc"})
@CapabilityDescription("Upsert the data of specified nodes on the OPC UA server. Nodes are of the form: 'ns;id;type;value'")
public class PutOPCData extends AbstractProcessor {

    // Attributes
    public static final PropertyDescriptor OPCUA_SERVICE = new PropertyDescriptor.Builder()
            .name("OPC UA Service")
            .description("Specifies the OPC UA Service that can be used to access data")
            .required(true)
            .identifiesControllerService(OPCUAService.class)
            .sensitive(false)
            .build();

    public static final PropertyDescriptor SKIP_NULL_VALUE = new PropertyDescriptor
            .Builder().name("Skip null values")
            .description("If true, lines containing empty values will be skipped. Otherwise a null value will be written.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("true")
            .build();

    public static final PropertyDescriptor USE_TIMESTAMP = new PropertyDescriptor
            .Builder().name("Use timestamp")
            .description("If true, it also writes the current Source and Server timestamp. Otherwise it writes a null value.")
            .required(true)
            .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
            .allowableValues("true", "false")
            .defaultValue("false")
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

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final List<PropertyDescriptor> descriptors = new ArrayList<>();
        descriptors.add(OPCUA_SERVICE);
        descriptors.add(SKIP_NULL_VALUE);
        descriptors.add(USE_TIMESTAMP);
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
                List<Tuple3<String,String, Object>> values = new Vector<Tuple3<String, String, Object>>();

                try {
                    // Read FlowFile content and send to OPC UA service to upsert data
                    // If the callback fails, it will be handled by the surrounding try-catch statement
                    session.read(flowFile, in -> {
                        try(final InputStreamReader isr = new InputStreamReader(in)){
                            // Read the content of the FlowFile and split by EndOfLine separator
                            List<String> lines = new BufferedReader(isr).lines().collect(Collectors.toList());
                            // Extract all the key/value pairs
                            for(String line : lines){
                                String[] tokens = line.replace("\r", "").split(context.getProperty(DELIMITER_CHARACTER).toString());
                                if(tokens.length == 4){
                                    Object value = BuiltInDataType.build(tokens[2], tokens[3]);
                                    values.add(new Tuple3(tokens[0], tokens[1], value));
                                }
                                else if(tokens.length == 3 && !context.getProperty(SKIP_NULL_VALUE).asBoolean()){
                                    values.add(new Tuple3(tokens[0], tokens[1], null));
                                }
                                else if(tokens.length == 3 && context.getProperty(SKIP_NULL_VALUE).asBoolean()){
                                    ;; //skip
                                }
                                else
                                    throw new RuntimeException("Exactly 4 values are required: namespace index, node identifier, value datatype and node value separated by 'Delimiter Character'. Found " + tokens.length + " in: '" + line + "'.");
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
                    Boolean withTimestamp = context.getProperty(USE_TIMESTAMP).asBoolean();
                    // Write values to OPC UA service
                    for (Tuple3<String, String, Object> v : values){
                        opcUAService.putValue(v.v1, v.v2, v.v3, withTimestamp);
                    }
                    // TODO write one by one!!!!
                    //opcUAService.putValues(values);
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
