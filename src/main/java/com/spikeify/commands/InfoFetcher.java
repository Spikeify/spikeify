package com.spikeify.commands;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * A command chain for fetching various information from database.
 */
public class InfoFetcher {

	protected final IAerospikeClient synClient;

	public static final String CONFIG_SET_NAME = "set_name";
	public static final String CONFIG_NS_NAME = "ns_name";
	public static final String CONFIG_SET_PARAM = "sets";
	public static final String CONFIG_NAMESPACES_PARAM = "namespaces";
	public static final String CONFIG_RECORDS_COUNT = "n_objects";

	public InfoFetcher(IAerospikeClient synClient) {
		this.synClient = synClient;
	}

	/**
	 * Returns the number of records in given namespace and set.
	 * @param namespace
	 * @param setName
	 * @return
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
	 * @return A map of sets and the namespaces they belong to
	 */
	public Map<String /** set **/, String /** namespace **/> getSets() {

		Map<String, String> setNames = new HashMap();

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

	private Map<String, String> parseConfigString(String configString) {
		Map<String, String> result = new HashMap<>();
		String[] chunks = configString.split(":");
		for (String chunk : chunks) {
			String[] chunkParts = chunk.split("=");
			result.put(chunkParts[0], chunkParts[1]);
		}
		return result;
	}

}
