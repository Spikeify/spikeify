package com.spikeify;

import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Info;
import com.aerospike.client.cluster.Node;

public class InfoFetcher {

	protected final IAerospikeClient synClient;

	public InfoFetcher(IAerospikeClient synClient) {
		this.synClient = synClient;
	}


	public String getSets() {
		Node[] nodes = synClient.getNodes();
		for (Node node : nodes) {
			String nodeSets = Info.request(null, node, "sets");
			System.out.println();
		}

		return null;
	}


}
