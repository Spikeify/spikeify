package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;
import com.aerospike.client.query.IndexCollectionType;
import com.aerospike.client.query.IndexType;
import com.spikeify.IndexingService;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A command chain for fetching various information from database.
 */
@SuppressWarnings("WeakerAccess")
public class InfoFetcher {

	protected final IAerospikeClient synClient;

	public static final String CONFIG_SET_NAME = "set_name";
	public static final String CONFIG_NS_NAME = "ns_name";
	public static final String CONFIG_SET_PARAM = "sets";
	public static final String CONFIG_SET_INDEXES = "sindex";
	public static final String CONFIG_NAMESPACES_PARAM = "namespaces";
	public static final String CONFIG_RECORDS_COUNT = "n_objects";

	public InfoFetcher(IAerospikeClient synClient) {

		this.synClient = synClient;
	}

	/**
	 * Returns the number of records in given namespace and set.
	 *
	 * @param namespace The namespace.
	 * @param clazz The name of the set.
	 * @return number of records in given namespace and set
	 */
	public int getRecordCount(String namespace, Class clazz) {
		return getRecordCount(namespace, IndexingService.getSetName(clazz));
	}

	/**
	 * Returns the number of records in given namespace and set.
	 *
	 * @param namespace The namespace.
	 * @param setName   The name of the set.
	 * @return number of records in given namespace and set
	 */
	public int getRecordCount(String namespace, String setName) {

		int count = 0;

		Node[] nodes = synClient.getNodes();
		for (Node node : nodes) {
			String nodeSets = Info.request(new InfoPolicy(), node, CONFIG_SET_PARAM + "/" + namespace + "/" + setName);
			String[] set = nodeSets.split(";");
			for (String setString : set) {
				Map<String, String> config = parseConfigString(setString);
				count += Integer.valueOf(config.get(CONFIG_RECORDS_COUNT));
			}
		}
		return count;
	}

	/**
	 * Fetches all namespaces available in database..
	 *
	 * @return A set of namespaces
	 */
	public Set<String> getNamespaces() {

		Set<String> nsNames = new HashSet<>();
		Node[] nodes = synClient.getNodes();
		for (Node node : nodes) {
			String nodeNsNames = Info.request(new InfoPolicy(), node, CONFIG_NAMESPACES_PARAM);
			nsNames.add(nodeNsNames);
		}
		return nsNames;
	}

	/**
	 * Fetches all Sets available in database.
	 *
	 * @return A map of sets and the namespaces they belong to
	 */
	public Map<String /** set **/, String /** namespace **/> getSets() {

		Map<String, String> setNames = new HashMap<>();

		Node[] nodes = synClient.getNodes();
		for (Node node : nodes) {
			String nodeSets = Info.request(new InfoPolicy(), node, CONFIG_SET_PARAM);
			String[] set = nodeSets.split(";");
			for (String setString : set) {
				Map<String, String> config = parseConfigString(setString);
				setNames.put(config.get(CONFIG_SET_NAME), config.get(CONFIG_NS_NAME));
			}
		}

		return setNames;
	}

	/**
	 * Fetches all indexes available in namespace..
	 * @param namespace namespace
	 * @return map of index name / {@link IndexInfo}
	 */
	public Map<String, IndexInfo> getIndexes(String namespace) {

		return getIndexes(namespace, (String)null);
	}

	/**
	 * Fetches all indexes of specific set available in namespace..
	 * @param namespace namespace
	 * @param clazz entity type
	 * @return map of index name / {@link IndexInfo}
	 */
	public Map<String, IndexInfo> getIndexes(String namespace, Class clazz) {
		return getIndexes(namespace, IndexingService.getSetName(clazz));
	}

	/**
	 * Fetches all indexes of specific set available in namespace..
	 * @param namespace namespace
	 * @param setName entity name / table name
	 * @return map of index info (index_name / info)
	 */
	public Map<String, IndexInfo> getIndexes(String namespace, String setName) {

		Map<String, IndexInfo> indexInfoSet = new HashMap<>();
		Node[] nodes = synClient.getNodes();
		for (Node node : nodes) {

			String indexInfo = Info.request(new InfoPolicy(), node, CONFIG_SET_INDEXES + "/" + namespace);
			String[] set = indexInfo.split(";");

			for (String setString : set) {
				IndexInfo info = new IndexInfo(setString);

				if (setName == null || setName.equals(info.setName)) {
					indexInfoSet.put(info.name, info);
				}
			}
		}

		return indexInfoSet;
	}

	private Map<String, String> parseConfigString(String configString) {

		Map<String, String> result = new HashMap<>();
		String[] chunks = configString.split(":");
		for (String chunk : chunks) {
			String[] chunkParts = chunk.split("=");
			if (chunkParts.length == 2) {
				result.put(chunkParts[0], chunkParts[1]);
			}
		}
		return result;
	}

	/**
	 * Holds information about index
	 */
	public class IndexInfo {

		public String namespace;

		public String setName;

		public String fieldName;

		public String name;

		public IndexType indexType;

		public IndexCollectionType collectionType;

		public boolean synced;
		public boolean canRead;
		public boolean canWrite;

		public IndexInfo(String nodeInfo) {

			// index info is parsed from string ... in format
			// ns=test:set=EntityOne:indexname=index_one:num_bins=1:bins=one:type=INT SIGNED:indextype=NONE:path=one:sync_state=synced:state=RW;
			Map<String, String> chunks = parseConfigString(nodeInfo);
			for (String key : chunks.keySet()) {
				String value = chunks.get(key);

				switch (key) {
					case "ns" :
						namespace = value;
						break;

					case "set":
						setName = value;
						break;

					case "indexname":
						name = value;
						break;

					case "path":
						fieldName = value;
						break;

					case "type":
						if ("TEXT".equals(value)) {
							indexType = IndexType.STRING;
						}
						else {  // "INT SIGNED"
							indexType = IndexType.NUMERIC;
						}
						break;

					case "indextype":

						try {
							collectionType = IndexCollectionType.valueOf(value);
						}
						catch (IllegalArgumentException e) {
							collectionType = IndexCollectionType.DEFAULT;
						}

						break;

					case "sync_state" :
						synced = "synced".equals(value);
						break;

					case "state" :
						canRead = value.contains("R");
						canWrite = value.contains("W");
						break;
				}
			}
		}
	}
}
