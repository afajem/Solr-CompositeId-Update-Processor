# Solr Composite ID Update Processor

##Overview
This utility should be used to generate a composite ID during the document
update/create process. Composite IDs (aka shard keys) are used during Solr 
index to distribute documents across shards that are a part of a Solr
document collection.

A useful reason to have a composite ID is that it assists in partitioning
Solr documents into specific shards depending on the desired usage/access requirements. So 
for instance, if a specific shard should contain a type of entity, then the same shard keys 
should be used for documents that match that type of document. Once configured, the class
will be executed as a part of the update chain ensuring that the composite id is generated
and updated in the Solr document that is passed along the update chain.

The designated format of a shard key is :
<pre>&lt;shard_key&gt;!&lt;document_id&gt;</pre> 

where the <code>&lt;shard_key&gt;</code> 
is a value that will be hashed during the distributed indexing to determine which shard
the document belongs to. The <code>&lt;document_id&gt;</code> is essentially a unique identifier for the document.

##The Code

The utility is encapsulated into a class called <code>CompositeIdUpdateProcessorFactory</code>. This 
class extends the <code>UpdateRequestProcessorFactory</code> class, providing an implementation for 
the factory method <code>getInstance()</code>. The entire code base is an Eclipse project that can 
either be imported into the Eclipse IDE or simply used as-is. 

To build the code, simply run the following Maven build command: <code>mvn package</code>. A JAR file 
should be created in the <code>target</code> folder. Copy this JAR file into the <code>lib</code> folder of your Solr
core. If you have a multi-core deployment, then ensure that the file is placed at a location that is 
accessible by all the cores.

## Configuring the Processor in Solr
The class is configurable in the <code>solrconfig.xml</code> file of Solr, as a part of an 
<code>&lt;updateRequestProcessorChain&gt;</code> definition. The update chain will need to 
be referenced by a request handler that is also defined in the <code>solrconfig.xml</code> 
file. For more information about Solr's Document Duplication Detection, see the following 
link that goes into Deduplication and how it relates to Solr: http://wiki.apache.org/solr/Deduplication

Below is a sample definition of the configuration for the update processor factory:

``` 
 <updateRequestProcessorChain name="myDedupe">
 	...
 	<processor class="com.niraninteractive.solr.processor.CompositeIdUpdateProcessorFactory">
 		<str name="compositeIdField">id</str>
 		<str name="prefixFields">entityType</str>
 		<str name="postfixField">id</str>
 		<bool name="overwriteDupes">false</bool>
 	</processor>		
 	...
 	<processor class="solr.LogUpdateProcessorFactory" />
 	<processor class="solr.RunUpdateProcessorFactory" />
 </updateRequestProcessorChain>
``` 

 The configuration above specifies an update chain with one of the update processor factory 
 classes, being the <code>CompositeIdUpdateProcessorFactory</code>. The definition takes the
 following parameters:
 * <code>compositeIdField</code> - Name of the field that will be used to store 
 the resulting composite id.
 * <code>prefixFields</code> - A comma delimited list of document fields that will be 
 concatenated together to form the shard key.
 * <code>postfixField</code> - The field name of the unique document id that should be appended to the shard key 
 to form the composite id.
 * <code>overwriteDupes</code> (optional) - A boolean indicating if duplicates should be 
 overwritten or skipped. Default value is <code>true</code>.
 * <code>enabled</code> (optional) - A boolean indicating if the update processor factory
 is enabled. Default value is <code>true</code>.
 
Once properly configured, simply index a few documents and query the index to ensure that 
the ids of the documents are specified using the composite id format.

Hope someone finds this utility as useful as I did. Please do not hesitate to reach out if you have any questions. 

Cheers!