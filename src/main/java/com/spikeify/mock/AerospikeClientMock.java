package com.spikeify.mock;

import com.aerospike.client.*;
import com.aerospike.client.admin.Privilege;
import com.aerospike.client.admin.Role;
import com.aerospike.client.admin.User;
import com.aerospike.client.cluster.Node;
import com.aerospike.client.command.ParticleType;
import com.aerospike.client.large.LargeList;
import com.aerospike.client.large.LargeMap;
import com.aerospike.client.large.LargeSet;
import com.aerospike.client.large.LargeStack;
import com.aerospike.client.policy.*;
import com.aerospike.client.query.*;
import com.aerospike.client.task.ExecuteTask;
import com.aerospike.client.task.IndexTask;
import com.aerospike.client.task.RegisterTask;
import com.spikeify.SpikeifyError;

import java.util.*;

@SuppressWarnings({"WeakerAccess", "UnusedAssignment", "deprecation"})
public class AerospikeClientMock implements IAerospikeClient {

	public final Policy readPolicyDefault = new Policy();
	public final WritePolicy writePolicyDefault = new WritePolicy();
	public ScanPolicy scanPolicyDefault = new ScanPolicy();
	public QueryPolicy queryPolicyDefault = new QueryPolicy();
	public BatchPolicy batchPolicyDefault = new BatchPolicy();

	public final String defaultNamespace;

	private final List<String> nodes = Collections.singletonList("node1");

	public AerospikeClientMock(String defaultNamespace) {
		this.defaultNamespace = defaultNamespace == null ? "<none>" : defaultNamespace;
	}

	@SuppressWarnings("SameParameterValue")
	public static class Rec {
		public int generation;
		public int expires;
		public Long userKeyLong;
		public String userKeyString;
		final Map<String /**property name**/, Object> bins = new HashMap<>();

		public Map<String, Object> getBins() {
			return bins;
		}

		private void updateBins(int expires, Bin... newBins) {
			this.expires = expires;
			generation++;
			for (Bin newBin : newBins) {


				Object obj = newBin.value.getObject();

				if (obj.getClass().equals(Integer.class)) {
					bins.put(newBin.name, Long.valueOf((Integer) obj));
				} else if (obj.getClass().equals(Long.class)) {
					bins.put(newBin.name, obj);
				} else if (obj.getClass().equals(Double.class)) {
					bins.put(newBin.name, Double.doubleToLongBits((Double) obj));
				} else if (obj.getClass().equals(Float.class)) {
					bins.put(newBin.name, Double.doubleToLongBits((Float) obj));
				} else if (obj.getClass().equals(String.class)) {
					bins.put(newBin.name, obj);
				} else if (obj.getClass().equals(Boolean.class)) {
					bins.put(newBin.name, (Boolean) obj ? 1L : 0L);
				} else if (obj.getClass().equals(Short.class)) {
					bins.put(newBin.name, Long.valueOf((Short) obj));
				} else if (obj.getClass().equals(Byte.class)) {
					bins.put(newBin.name, Long.valueOf((Byte) obj));
				} else if (obj.getClass().equals(Date.class)) {
					bins.put(newBin.name, ((Date) obj).getTime());
				} else if (List.class.isAssignableFrom(obj.getClass())) {
					bins.put(newBin.name, obj);
				} else if (Map.class.isAssignableFrom(obj.getClass())) {
					bins.put(newBin.name, obj);
				} else {
					throw new SpikeifyError("Not yet supported type: " + obj.getClass());
				}

			}
		}
	}

	private Map<String/**namespace**/, Map<String/**set**/, Map<Key, Rec>>> db =
			new HashMap<>(3);

	private Map<String/**set**/, Map<Key, Rec>> getNamespace(String namespace) {
		Map<String, Map<Key, Rec>> ns = db.get(namespace);
		if (ns == null) {
			ns = new HashMap<>();
			db.put(namespace, ns);
		}
		return ns;
	}

	private Map<Key, Rec> getSet(String setName, Map<String/**set**/, Map<Key, Rec>> namespace) {
		Map<Key, Rec> set = namespace.get(setName);
		if (set == null) {
			set = new HashMap<>();
			namespace.put(setName, set);
		}
		return set;
	}


	@Override
	public void close() {
		db = null;
	}

	@Override
	public boolean isConnected() {
		return db != null;
	}

	@Override
	public Node[] getNodes() {
		return null;
	}

	@Override
	public List<String> getNodeNames() {
		return nodes;
	}

	@Override
	public Node getNode(String nodeName) throws AerospikeException.InvalidNode {
		return null;
	}

	@Override
	public void put(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {
		policy = (policy == null) ? writePolicyDefault : policy;
		String nsName = key.namespace == null ? defaultNamespace : key.namespace;
		String setName = key.setName == null ? defaultNamespace : key.setName;

		Map<String, Map<Key, Rec>> namespace = getNamespace(nsName);
		Map<Key, Rec> set = getSet(setName, namespace);
		Rec existingRec = set.get(key);

		switch (policy.recordExistsAction) {
			case UPDATE:
				existingRec = (existingRec == null) ? new Rec() : existingRec;
				existingRec.updateBins(0, bins);
				break;
			case UPDATE_ONLY:
				if (existingRec == null) {
					throw new AerospikeException("RecordExistsAction." + policy.recordExistsAction.name() + ": record does not exist");
				}
				existingRec.updateBins(0, bins);
				break;
			case REPLACE:
				existingRec = new Rec();
				existingRec.updateBins(0, bins);
				break;
			case REPLACE_ONLY:
				if (existingRec == null) {
					throw new AerospikeException("RecordExistsAction." + policy.recordExistsAction.name() + ": record does not exist");
				}
				existingRec = new Rec();
				existingRec.updateBins(0, bins);
				break;
			case CREATE_ONLY:
				if (existingRec != null) {
					throw new AerospikeException("RecordExistsAction." + policy.recordExistsAction.name() + ": record already exists");
				}
				existingRec = new Rec();
				existingRec.updateBins(0, bins);
				break;
		}

		// set UserKey field
		switch (key.userKey.getType()) {
			case ParticleType.STRING:
				existingRec.userKeyString = key.userKey.toString();
				break;
			case ParticleType.INTEGER:
				existingRec.userKeyLong = key.userKey.toLong();
				break;
		}
		set.put(key, existingRec);

	}

	@Override
	public void append(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {

	}

	@Override
	public void prepend(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {

	}

	@Override
	public void add(WritePolicy policy, Key key, Bin... bins) throws AerospikeException {

	}

	@Override
	public boolean delete(WritePolicy policy, Key key) throws AerospikeException {
		String nsName = key.namespace == null ? defaultNamespace : key.namespace;
		String setName = key.setName == null ? defaultNamespace : key.setName;

		Map<String, Map<Key, Rec>> namespace = getNamespace(nsName);
		Map<Key, Rec> set = getSet(setName, namespace);
		Rec removed = set.remove(key);
		return removed != null;
	}

	@Override
	public void touch(WritePolicy policy, Key key) throws AerospikeException {

	}

	@Override
	public boolean exists(Policy policy, Key key) throws AerospikeException {
		policy = (policy == null) ? readPolicyDefault : policy;
		String nsName = key.namespace == null ? defaultNamespace : key.namespace;
		String setName = key.setName == null ? defaultNamespace : key.setName;

		Map<String, Map<Key, Rec>> namespace = getNamespace(nsName);
		Map<Key, Rec> set = getSet(setName, namespace);
		return set.containsKey(key);
	}

	@Override
	public boolean[] exists(Policy policy, Key[] keys) throws AerospikeException {
		return new boolean[0];
	}

	@Override
	public boolean[] exists(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return new boolean[0];
	}

	@Override
	public Record get(Policy policy, Key key) throws AerospikeException {
		policy = (policy == null) ? readPolicyDefault : policy;
		String nsName = key.namespace == null ? defaultNamespace : key.namespace;
		String setName = key.setName == null ? defaultNamespace : key.setName;

		Map<String, Map<Key, Rec>> namespace = getNamespace(nsName);
		Map<Key, Rec> set = getSet(setName, namespace);
		Rec existingRec = set.get(key);

		if (existingRec == null) {
			return null;
		}

		return new Record(existingRec.getBins(), existingRec.generation, existingRec.expires);
	}

	@Override
	public Record get(Policy policy, Key key, String... binNames) throws AerospikeException {
		return null;
	}

	@Override
	public Record getHeader(Policy policy, Key key) throws AerospikeException {
		return null;
	}

	@Override
	public void get(BatchPolicy batchPolicy, List<BatchRead> list) throws AerospikeException {

	}

	@Override
	public Record[] get(Policy policy, Key[] keys) throws AerospikeException {
		return new Record[0];
	}

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys) throws AerospikeException {
		Record[] records = new Record[keys.length];
		for (int i = 0; i < keys.length; i++) {
			Key key = keys[i];
			records[i] = get(policy, key);
		}
		return records;
	}

	@Override
	public Record[] get(Policy policy, Key[] keys, String... binNames) throws AerospikeException {
		return new Record[0];
	}

	@Override
	public Record[] get(BatchPolicy policy, Key[] keys, String... binNames) throws AerospikeException {
		return new Record[0];
	}

	@Override
	public Record[] getHeader(Policy policy, Key[] keys) throws AerospikeException {
		return new Record[0];
	}

	@Override
	public Record[] getHeader(BatchPolicy policy, Key[] keys) throws AerospikeException {
		return new Record[0];
	}

	@Override
	public Record operate(WritePolicy policy, Key key, Operation... operations) throws AerospikeException {
		return null;
	}

	@Override
	public void scanAll(ScanPolicy policy, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {

	}

	@Override
	public void scanNode(ScanPolicy policy, String nodeName, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {

	}

	@Override
	public void scanNode(ScanPolicy policy, Node node, String namespace, String setName, ScanCallback callback, String... binNames) throws AerospikeException {

	}

	@Override
	public LargeList getLargeList(Policy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public LargeList getLargeList(WritePolicy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public LargeList getLargeList(WritePolicy writePolicy, Key key, String s) {
		return null;
	}

	@Override
	public LargeMap getLargeMap(Policy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public LargeMap getLargeMap(WritePolicy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public LargeSet getLargeSet(Policy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public LargeSet getLargeSet(WritePolicy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public LargeStack getLargeStack(Policy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public LargeStack getLargeStack(WritePolicy policy, Key key, String binName, String userModule) {
		return null;
	}

	@Override
	public RegisterTask register(Policy policy, String clientPath, String serverPath, Language language) throws AerospikeException {
		return null;
	}

	@Override
	public RegisterTask register(Policy policy, ClassLoader resourceLoader, String resourcePath, String serverPath, Language language) throws AerospikeException {
		return null;
	}

	@Override
	public void removeUdf(InfoPolicy infoPolicy, String s) throws AerospikeException {

	}

	@Override
	public Object execute(Policy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
		return null;
	}

	@Override
	public Object execute(WritePolicy policy, Key key, String packageName, String functionName, Value... args) throws AerospikeException {
		return null;
	}

	@Override
	public ExecuteTask execute(Policy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
		return null;
	}

	@Override
	public ExecuteTask execute(WritePolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
		return null;
	}

	@Override
	public RecordSet query(QueryPolicy policy, Statement statement) throws AerospikeException {
		return null;
	}

	@Override
	public RecordSet queryNode(QueryPolicy policy, Statement statement, Node node) throws AerospikeException {
		return null;
	}

	@Override
	public ResultSet queryAggregate(QueryPolicy policy, Statement statement, String packageName, String functionName, Value... functionArgs) throws AerospikeException {
		return null;
	}

	@Override
	public ResultSet queryAggregate(QueryPolicy policy, Statement statement) throws AerospikeException {
		return null;
	}

	@Override
	public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType) throws AerospikeException {
		return null;
	}

	@Override
	public IndexTask createIndex(Policy policy, String namespace, String setName, String indexName, String binName, IndexType indexType, IndexCollectionType indexCollectionType) throws AerospikeException {
		return null;
	}

	@Override
	public void dropIndex(Policy policy, String namespace, String setName, String indexName) throws AerospikeException {

	}

	@Override
	public void createUser(AdminPolicy policy, String user, String password, List<String> roles) throws AerospikeException {

	}

	@Override
	public void dropUser(AdminPolicy policy, String user) throws AerospikeException {

	}

	@Override
	public void changePassword(AdminPolicy policy, String user, String password) throws AerospikeException {

	}

	@Override
	public void grantRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {

	}

	@Override
	public void revokeRoles(AdminPolicy policy, String user, List<String> roles) throws AerospikeException {

	}

	@Override
	public void createRole(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {

	}

	@Override
	public void dropRole(AdminPolicy policy, String roleName) throws AerospikeException {

	}

	@Override
	public void grantPrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {

	}

	@Override
	public void revokePrivileges(AdminPolicy policy, String roleName, List<Privilege> privileges) throws AerospikeException {

	}

	@Override
	public User queryUser(AdminPolicy policy, String user) throws AerospikeException {
		return null;
	}

	@Override
	public List<User> queryUsers(AdminPolicy policy) throws AerospikeException {
		return null;
	}

	@Override
	public Role queryRole(AdminPolicy policy, String roleName) throws AerospikeException {
		return null;
	}

	@Override
	public List<Role> queryRoles(AdminPolicy policy) throws AerospikeException {
		return null;
	}
}
