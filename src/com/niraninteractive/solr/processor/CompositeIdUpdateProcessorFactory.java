package com.niraninteractive.solr.processor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.index.Term;
import org.apache.solr.common.SolrException;
import org.apache.solr.common.SolrInputDocument;
import org.apache.solr.common.SolrException.ErrorCode;
import org.apache.solr.common.params.SolrParams;
import org.apache.solr.common.util.NamedList;
import org.apache.solr.common.util.StrUtils;
import org.apache.solr.core.SolrCore;
import org.apache.solr.request.SolrQueryRequest;
import org.apache.solr.response.SolrQueryResponse;
import org.apache.solr.schema.SchemaField;
import org.apache.solr.update.AddUpdateCommand;
import org.apache.solr.update.processor.UpdateRequestProcessor;
import org.apache.solr.update.processor.UpdateRequestProcessorFactory;
import org.apache.solr.util.plugin.SolrCoreAware;

/**
 * This class should be used to generate a composite ID during the document
 * update/create process. Composite IDs (aka shard keys) are used during Solr 
 * index to distribute documents across shards that are a part of a Solr
 * document collection.
 * <p>
 *  A useful reason to have a composite ID is that it assists in partitioning
 *  Solr documents into specific shards depending on the desired usage/access requirements. So 
 *  for instance, if a specific shard should contain a type of entity, then the same shard keys 
 *  should be used for documents that match that type of document. Once configured, the class
 *  will be executed as a part of the update chain ensuring that the composite id is generated
 *  and updated in the Solr document that is passed along the update chain.
 * <p>
 * The designated format of a shard key is :
 * <code>&lt;shard_key&gt;!&lt;document_id&gt;</code>, where the <code>&lt;shard_key&gt;</code> 
 * is a value that will be hashed during the distributed indexing to determine which shard
 * the document belongs to. The <code>&lt;document_id&gt;</code> is essentially a unique identifier for the document.
 * <p> 
 * The class is configurable in the <code>solrconfig.xml</code> as a part of an 
 * <code>&lt;updateRequestProcessorChain&gt;</code> definition. Below is a sample definition 
 * of the configuration for the update processor factory:
 * 
 * <pre> 
 *	&lt;updateRequestProcessorChain name="myDedupe"&gt;
 *		...
 *		&lt;processor class="com.niraninteractive.solr.processor.CompositeIdUpdateProcessorFactory"&gt;
 *			&lt;str name="compositeIdField"&gt;id&lt;/str&gt;
 *			&lt;str name="prefixFields"&gt;entityType&lt;/str&gt;
 *			&lt;str name="postfixField"&gt;id&lt;/str&gt;
 *			&lt;bool name="overwriteDupes"&gt;false&lt;/bool&gt;
 *		&lt;/processor&gt;		
 *		...
 *		&lt;processor class="solr.LogUpdateProcessorFactory" /&gt;
 *		&lt;processor class="solr.RunUpdateProcessorFactory" /&gt;
 *	&lt;/updateRequestProcessorChain&gt;
 * </pre> 
 * 
 *  The configuration above specifies an update chain with one of the update processor factory 
 *  classes, being the <code>CompositeIdUpdateProcessorFactory</code>. The definition takes the
 *  following parameters:
 *  <li><code>compositeIdField</code> - Name of the field that will be used to store 
 *  the resulting composite ID</li>
 *  <li><code>prefixFields</code> - A comma delimited list of document fields that will be 
 *  concatenated together to form the shard key.</li>
 *  <li><code>postfixField</code> - The field name of the unique document id that should be appended to the shard key 
 *  to form the composite id</li>
 *  <li><code>overwriteDupes</code> (optional) - A boolean indicating if duplicates should be 
 *  overwritten or skipped. Default value is <code>true</code>.</li>
 *  <li><code>enabled</code> (optional) - A boolean indicating if the update processor factory
 *  is enabled. Default value is <code>true</code>.</li>
 * 
 * @author afajem
 */
public class CompositeIdUpdateProcessorFactory extends
		UpdateRequestProcessorFactory implements SolrCoreAware {
	
	/** The shard key separator. The exclamation point character is used internally by Solr */
	private final static String SHARD_KEY_SEPARATOR = "!";

	/** The postfix field configuration parameter passed into the class */
	private String postfixField;
	/** The prefix fields configuration parameter passed into the class */
	private List<String> prefixFields;
	/** The composite id field name that is passed in to the class as a configuration parameter */
	private String compositeIdField;
	/** The flag specify if duplicates will be overwritten, passed as a configuration parameter */
	private boolean overwriteDupes;
	/** The flag indicating if the class is enabled or not */
	private boolean enabled;
	
	/** Use to store schema field data */
	private Map<String, SchemaField> schemaFields = new HashMap<String, SchemaField>();

	/**
	 * Read in the configuration parameter (arguments) and initialize the class
	 * 
	 * @param args is a <code>NamedList</code> of configuration parameters that
	 * 		are passed into the class.
	 */
	@Override
	public void init(@SuppressWarnings("rawtypes") final NamedList args) {
		if (args != null) {
			SolrParams params = SolrParams.toSolrParams(args);

			overwriteDupes = params.getBool("overwriteDupes", true);

			compositeIdField = params.get("compositeIdField", "compositeIdField");

			prefixFields = 
				StrUtils.splitSmart(
					params.get("prefixFields", "prefixFields"), ',');
			if (prefixFields != null) {
				Collections.sort(prefixFields);
			}
			
			postfixField = params.get("postfixField", "postfixField");

			enabled = params.getBool("enabled", true);
		}
	}
	
	
	
	/**
	 * Factory method to created a new {@link CompositeIdUpdateProcessor}
	 * 
	 */
	@Override
	public UpdateRequestProcessor getInstance(SolrQueryRequest request,
			SolrQueryResponse response, UpdateRequestProcessor nextProcessor) {
		return new CompositeIdUpdateProcessor(
					request, response, this, nextProcessor);
	}

	
	/**
	 * Perform some field level validation against the schema.
	 * 
	 * @param core the Solr Core
	 */
	@Override
	public void inform(SolrCore core) {
		
		//Validate that we have a valid prefix field(s) specified
		boolean prefixFieldsIndexed = false;
		if (prefixFields != null && !prefixFields.isEmpty()) {			
			for (String prefixField : prefixFields) {
				SchemaField prefixSchemaField = core.getSchema().getFieldOrNull(
						prefixField);
				if (prefixSchemaField == null) {
					throw new SolrException(ErrorCode.SERVER_ERROR,
						"Can't use a prefixField which does not exist in schema: "
									+ prefixField);
				}
				else {
					schemaFields.put(prefixField, prefixSchemaField);
				}
			}
			
			//Assume that the prefix fields passed the validation above
			prefixFieldsIndexed = true;
		}
		else {
			throw new SolrException(ErrorCode.SERVER_ERROR,
				"The prefix fields must be specified");
		}

		final SchemaField postfixSchemaField = core.getSchema().getFieldOrNull(
				getPostfixField());
		if (postfixSchemaField == null) {
			throw new SolrException(ErrorCode.SERVER_ERROR,
				"Can't use postfixField which does not exist in schema: "
							+ getPostfixField());
		}

		final SchemaField compositeIdSchemaField = core.getSchema().getFieldOrNull(
				getCompositeIdField());
		if (compositeIdSchemaField == null) {
			throw new SolrException(ErrorCode.SERVER_ERROR,
				"Can't use compositeIdField which does not exist in schema: "
							+ getCompositeIdField());
		}

		if (getOverwriteDupes() && 
			(!postfixSchemaField.indexed() || !prefixFieldsIndexed)) {
			throw new SolrException(ErrorCode.SERVER_ERROR,
				"Can't set overwriteDupes when either prefixFields or postfixField are not indexed: "
					+ "prefixFields=" + getPrefixFields()
					+ " postfixField=" + getPostfixField());
		}
	}

	
	/**
	 * Returns a handle of the id field for the composite key
	 * 
	 * @return the composite id field
	 */
	public String getCompositeIdField() {
		return compositeIdField;
	}


	/**
	 * Returns a handle on the prefix fields for the composite key
	 * 
	 * @return the prefix fields
	 */
	public List<String> getPrefixFields() {
		return prefixFields;
	}
	

	/**
	 * Returns a handle on the postfix field for the composite key
	 * 
	 * @return the postfix field
	 */
	public String getPostfixField() {
		return postfixField;
	}
	

	/**
	 * Return flag determining if dedupes can be overwritten.
	 * @return
	 */
	public boolean getOverwriteDupes() {
		return overwriteDupes;
	}	

	
	/**
	 * Return the flag indicating that the update processor factory
	 * is enabled.
	 * @return
	 */
	public boolean isEnabled() {
		return enabled;
	}


	/**
	 * Sets the value of the flag indicating whether or not the update 
	 * processor factory is enabled.
	 * @param enabled
	 */
	public void setEnabled(boolean enabled) {
		this.enabled = enabled;
	}


	/**
	 * The update processor used to create the composite key
	 * 
	 */
	class CompositeIdUpdateProcessor extends UpdateRequestProcessor {
		
		public CompositeIdUpdateProcessor(SolrQueryRequest req,
				SolrQueryResponse rsp, CompositeIdUpdateProcessorFactory factory,
				UpdateRequestProcessor next) {
			super(next);
		}


		/**
		 * Handles the add/update request received by the processor
		 * 
		 */
	    @Override
	    public void processAdd(AddUpdateCommand cmd) throws IOException {
	    	
	    	// Only proceed if the factory is enabled
	    	if (enabled) {
		        SolrInputDocument document = cmd.getSolrInputDocument();
		        
		        StringBuffer prefixFieldsValue = new StringBuffer();
		        for (String prefixFieldName : prefixFields) {
		        	String prefixFieldValue = String.valueOf(document.getFieldValue(prefixFieldName));
		        	
			        if (!prefixFieldValue.equals("null") && (prefixFieldValue.trim().length() > 0)) {
		        		prefixFieldsValue.append(prefixFieldValue);
			        }
			        else {
						throw new SolrException(ErrorCode.SERVER_ERROR,
							"A prefix field must not be empty or null as it's used as a part of a composite id. " +
							"Detected the following prefix field is empty or null: "+ prefixFieldName);
			        }
		        }
	
		        String postFixFieldValue = String.valueOf(document.getFieldValue(postfixField));
		        
		        //Perform null check on prefix and postfix
		        if ((prefixFieldsValue.toString().length() > 0) &&
		        	!postFixFieldValue.equals("null") && (postFixFieldValue.trim().length() > 0)) {
		        	//Add/Update composite id in document
		        	String compositeIdFieldValue = 
		        		prefixFieldsValue.toString() + SHARD_KEY_SEPARATOR + postFixFieldValue;
		        	
		        	document.setField(compositeIdField, compositeIdFieldValue);
	
		        	if (overwriteDupes) {
			            cmd.updateTerm = new Term(compositeIdField, compositeIdFieldValue);
			        }
		        }
		        else {
					throw new SolrException(ErrorCode.SERVER_ERROR,
							"Both prefixFields and postfixField values must be non-null/empty. " +
							"Input values for these fields are: "
								+ "prefixFields=" + getPrefixFields()
								+ "postfixField=" + getPostfixField());
		        }
	    	}
	    	
	        //On to the next command?
			if (next != null) {
				next.processAdd(cmd);
			}
	    }
	}
}
