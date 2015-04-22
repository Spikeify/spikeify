package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.policy.InfoPolicy;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class InfoFetcher {

	protected final IAerospikeClient synClient;

	public static final String CONFIG_SET_NAME = "set_name";
	public static final String CONFIG_NS_NAME = "ns_name";
	public static final String CONFIG_SET_PARAM= "sets";
	public static final String CONFIG_NAMESPACES_PARAM= "namespaces";

	public InfoFetcher(IAerospikeClient synClient) {
		this.synClient = synClient;
	}


	public Set<String> getNamespaces() {
		Set<String> nsNames = new HashSet<>();
		Node[] nodes = synClient.getNodes();
		for (Node node : nodes) {
			String nodeNsNames = Info.request(new InfoPolicy(), node, CONFIG_NAMESPACES_PARAM);
			nsNames.add(nodeNsNames);
		}
		return nsNames;
	}

	public Set<String> getSets() {

		Set<String> setNames = new HashSet<>();

		Node[] nodes = synClient.getNodes();
		for (Node node : nodes) {
			String nodeSets = Info.request(new InfoPolicy(), node, CONFIG_SET_PARAM);
			String[] set = nodeSets.split(";");
			for (String setString : set) {
				Map<String, String> config = parseConfigString(setString);
				setNames.add(config.get(CONFIG_SET_NAME));
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
