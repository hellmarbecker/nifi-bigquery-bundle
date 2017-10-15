package org.apache.nifi.processors.bigquery;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigquery.BigQuery;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.processors.gcp.AbstractGCPProcessor;
import org.apache.nifi.processors.bigquery.utils.NotNullValuesHashMap;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"google cloud", "google", "bigquery", "get"})
@SupportsBatching
@CapabilityDescription("Runs a query against BigQuery. "
        + "Returns the query result as JSON.")
public class GetBigquery extends AbstractBigqueryProcessor {

    static final PropertyDescriptor TABLE = new PropertyDescriptor.Builder()
            .name("Bigquery Table")
            .description("The table id where store the data. The table must be exist on bigquery")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    static final PropertyDescriptor DATASET = new PropertyDescriptor.Builder()
            .name("Bigquery Dataset")
            .description("The dataset id where find the table. The dataset must be exist on bigquery")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final List<PropertyDescriptor> properties = Collections.unmodifiableList(
            Arrays.asList(SERVICE_ACCOUNT_CREDENTIALS_JSON, READ_TIMEOUT, CONNECTION_TIMEOUT, PROJECT, DATASET, TABLE));


    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        final String table = context.getProperty(TABLE).getValue();
        final String dataset = context.getProperty(DATASET).getValue();

        ObjectMapper mapper = new ObjectMapper();
        try {
            Map<String, Object> jsonDocument = mapper.readValue(session.read(flowFile), new TypeReference<NotNullValuesHashMap<String, Object>>() {
            });
            InsertAllRequest.RowToInsert rowToInsert = InsertAllRequest.RowToInsert.of(jsonDocument);
            BigQuery bigQuery = getBigQuery();

            if ( ... ) {
                session.transfer(flowFile, REL_FAILURE);
            } else {
                session.transfer(flowFile, REL_SUCCESS);
            }


        } catch (IOException ioe) {
            getLogger().error("IOException while reading JSON item: " + ioe.getMessage());
            session.transfer(flowFile, REL_FAILURE);
        }


    }
}
