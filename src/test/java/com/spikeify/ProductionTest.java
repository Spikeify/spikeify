package com.spikeify;

import com.aerospike.client.AerospikeClient;
import com.aerospike.client.Host;
import com.spikeify.commands.InfoFetcher;

public class ProductionTest {

	public static void main(String[] args) {

		String[] hostnames = new String[]{};
		Host[] hosts = new Host[hostnames.length];

		for (int i = 0; i < hostnames.length; i++) {
			String hostname = hostnames[i];
			hosts[i] = new Host(hostname, 3000);
		}

		AerospikeClient cl = new AerospikeClient(null, hosts);

		System.out.println("nodes:" + cl.getNodes().length);

		InfoFetcher info = new InfoFetcher(cl);

		System.out.println("recs:" + info.getRecordCount("test", "User"));

		System.out.println("namespace config:");
		for (String configLine : info.getNamespaceConfig("test")) {
			System.out.println(configLine);
		}
	}
}
