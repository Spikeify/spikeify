package com.spikeify;

import com.aerospike.client.Host;
import com.aerospike.client.async.AsyncClient;
import com.spikeify.commands.InfoFetcher;

public class ProductionTest {

	public static void main(String[] args) {

		String[] hostnames = new String[]{};
		Host[] hosts = new Host[hostnames.length];

		for (int i = 0; i < hostnames.length; i++) {
			String hostname = hostnames[i];
			hosts[i] = new Host(hostname, 3000);
		}

		AsyncClient client = new AsyncClient(null, hosts);

		System.out.println("nodes:" + client.getNodes().length);

		InfoFetcher info = new InfoFetcher(client);

		System.out.println("recs:" + info.getRecordCount("test", "User"));

		System.out.println("namespace config:");
		for (String configLine : info.getNamespaceConfig("test")) {
			System.out.println(configLine);
		}
	}
}
