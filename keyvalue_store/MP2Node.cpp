/**********************************
 * FILE NAME: MP2Node.cpp
 *
 * DESCRIPTION: MP2Node class definition
 **********************************/
#include "MP2Node.h"
#include "Message.h"

/**
 * constructor
 */
MP2Node::MP2Node(Member *memberNode, Params *par, EmulNet * emulNet, Log * log, Address * address) {
	this->memberNode = memberNode;
	this->par = par;
	this->emulNet = emulNet;
	this->log = log;
	ht = new HashTable();
	this->memberNode->addr = *address;
}

/**
 * Destructor
 */
MP2Node::~MP2Node() {
	delete ht;
	delete memberNode;
}

/**
 * FUNCTION NAME: updateRing
 *
 * DESCRIPTION: This function does the following:
 * 				1) Gets the current membership list from the Membership Protocol (MP1Node)
 * 				   The membership list is returned as a vector of Nodes. See Node class in Node.h
 * 				2) Constructs the ring based on the membership list
 * 				3) Calls the Stabilization Protocol
 */
void MP2Node::updateRing() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	vector<Node> curMemList;
	bool change = false;

	/*
	 *  Step 1. Get the current membership list from Membership Protocol / MP1
	 */
	curMemList = getMembershipList();

	/*
	 * Step 2: Construct the ring
	 */
	// Sort the list based on the hashCode
	sort(curMemList.begin(), curMemList.end());


	/*
	 * Step 3: Run the stabilization protocol IF REQUIRED
	 */
	// Run stabilization protocol if the hash table size is greater than zero and if there has been a changed in the ring
	ring = curMemList;
	stabilizationProtocol();
}

/**
 * FUNCTION NAME: getMemberhipList
 *
 * DESCRIPTION: This function goes through the membership list from the Membership protocol/MP1 and
 * 				i) generates the hash code for each member
 * 				ii) populates the ring member in MP2Node class
 * 				It returns a vector of Nodes. Each element in the vector contain the following fields:
 * 				a) Address of the node
 * 				b) Hash code obtained by consistent hashing of the Address
 */
vector<Node> MP2Node::getMembershipList() {
	unsigned int i;
	vector<Node> curMemList;
	for ( i = 0 ; i < this->memberNode->memberList.size(); i++ ) {
		Address addressOfThisMember;
		int id = this->memberNode->memberList.at(i).getid();
		short port = this->memberNode->memberList.at(i).getport();
		memcpy(&addressOfThisMember.addr[0], &id, sizeof(int));
		memcpy(&addressOfThisMember.addr[4], &port, sizeof(short));
		curMemList.emplace_back(Node(addressOfThisMember));
	}
	return curMemList;
}

/**
 * FUNCTION NAME: hashFunction
 *
 * DESCRIPTION: This functions hashes the key and returns the position on the ring
 * 				HASH FUNCTION USED FOR CONSISTENT HASHING
 *
 * RETURNS:
 * size_t position on the ring
 */
size_t MP2Node::hashFunction(string key) {
	std::hash<string> hashFunc;
	size_t ret = hashFunc(key);
	return ret%RING_SIZE;
}

/**
 * FUNCTION NAME: clientCreate
 *
 * DESCRIPTION: client side CREATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientCreate(string key, string value) {
	/*
	 * Implement this
	 */
	vector<Node> replicas = findNodes(key);
	int transID = ++g_transID;
	if (replicas.size() == 0) {
		log->logCreateFail(&(memberNode->addr), true, transID, key, value);
		return;
	} else {
		// send message
		Message m0(transID, memberNode->addr, CREATE, key, value, PRIMARY);
		Message m1(transID, memberNode->addr, CREATE, key, value, SECONDARY);
		emulNet->ENsend(&memberNode->addr, &replicas.at(0).nodeAddress, m0.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(1).nodeAddress, m1.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(2).nodeAddress, m1.toString());
		opSet.emplace(transID, m0);
	}
}

/**
 * FUNCTION NAME: clientRead
 *
 * DESCRIPTION: client side READ API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientRead(string key){
	/*
	 * Implement this
	 */
	vector<Node> replicas = findNodes(key);
	int transID = ++g_transID;
	if (replicas.size() == 0) {
		log->logReadFail(&(memberNode->addr), true, transID, key);
		return;
	} else {
		// send message
		Message m(transID, memberNode->addr, READ, key);
		emulNet->ENsend(&memberNode->addr, &replicas.at(0).nodeAddress, m.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(1).nodeAddress, m.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(2).nodeAddress, m.toString());
		opSet.emplace(transID, m);
	}
}

/**
 * FUNCTION NAME: clientUpdate
 *
 * DESCRIPTION: client side UPDATE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientUpdate(string key, string value){
	/*
	 * Implement this
	 */
	vector<Node> replicas = findNodes(key);
	int transID = ++g_transID;
	if (replicas.size() == 0) {
		log->logUpdateFail(&(memberNode->addr), true, transID, key, value);
		return;
	} else {
		// send message
		Message m0(transID, memberNode->addr, UPDATE, key, value, PRIMARY);
		Message m1(transID, memberNode->addr, UPDATE, key, value, SECONDARY);
		emulNet->ENsend(&memberNode->addr, &replicas.at(0).nodeAddress, m0.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(1).nodeAddress, m1.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(2).nodeAddress, m1.toString());
		opSet.emplace(transID, m0);
	}
}

/**
 * FUNCTION NAME: clientDelete
 *
 * DESCRIPTION: client side DELETE API
 * 				The function does the following:
 * 				1) Constructs the message
 * 				2) Finds the replicas of this key
 * 				3) Sends a message to the replica
 */
void MP2Node::clientDelete(string key){
	/*
	 * Implement this
	 */
	vector<Node> replicas = findNodes(key);
	int transID = ++g_transID;
	if (replicas.size() == 0) {
		log->logDeleteFail(&(memberNode->addr), true, transID, key);
		return;
	} else {
		// send message
		Message m(transID, memberNode->addr, DELETE, key);
		emulNet->ENsend(&memberNode->addr, &replicas.at(0).nodeAddress, m.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(1).nodeAddress, m.toString());
		emulNet->ENsend(&memberNode->addr, &replicas.at(2).nodeAddress, m.toString());
		opSet.emplace(transID, m);
	}
}

/**
 * FUNCTION NAME: createKeyValue
 *
 * DESCRIPTION: Server side CREATE API
 * 			   	The function does the following:
 * 			   	1) Inserts key value into the local hash table
 * 			   	2) Return true or false based on success or failure
 */
bool MP2Node::createKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Insert key, value, replicaType into the hash table
	return ht->create(key, value);
}

/**
 * FUNCTION NAME: readKey
 *
 * DESCRIPTION: Server side READ API
 * 			    This function does the following:
 * 			    1) Read key from local hash table
 * 			    2) Return value
 */
string MP2Node::readKey(string key) {
	/*
	 * Implement this
	 */
	// Read key from local hash table and return value
	return ht->read(key);
}

/**
 * FUNCTION NAME: updateKeyValue
 *
 * DESCRIPTION: Server side UPDATE API
 * 				This function does the following:
 * 				1) Update the key to the new value in the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::updateKeyValue(string key, string value, ReplicaType replica) {
	/*
	 * Implement this
	 */
	// Update key in local hash table and return true or false
	return ht->update(key, value);
}

/**
 * FUNCTION NAME: deleteKey
 *
 * DESCRIPTION: Server side DELETE API
 * 				This function does the following:
 * 				1) Delete the key from the local hash table
 * 				2) Return true or false based on success or failure
 */
bool MP2Node::deletekey(string key) {
	/*
	 * Implement this
	 */
	// Delete the key from the local hash table
	return ht->deleteKey(key);
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: This function is the message handler of this node.
 * 				This function does the following:
 * 				1) Pops messages from the queue
 * 				2) Handles the messages according to message types
 */
void MP2Node::checkMessages() {
	/*
	 * Implement this. Parts of it are already implemented
	 */
	char * data;
	int size;
	map<int, int> transSet;
	map<int, int>::iterator iter;
	string val;
	bool success;
	int quorum;
	Message * reply;

	/*
	 * Declare your local variables here
	 */

	// dequeue all messages and handle them
	while ( !memberNode->mp2q.empty() ) {
		/*
		 * Pop a message from the queue
		 */
		data = (char *)memberNode->mp2q.front().elt;
		size = memberNode->mp2q.front().size;
		memberNode->mp2q.pop();

		string message(data, data + size);

		/*
		 * Handle the message types here
		 */
		Message m(message);
		reply = NULL;
		switch (m.type)
		{
		case CREATE:
			success = createKeyValue(m.key, m.value, m.replica);
			if (m.replica == TERTIARY) {
				//break; // replica of stabilization protocol, just skip
			}
			if (success) {
				log->logCreateSuccess(&(memberNode->addr), false, m.transID, m.key, m.value);
			} else {
				log->logCreateFail(&(memberNode->addr), false, m.transID, m.key, m.value);
			}
			reply = new Message(m.transID, memberNode->addr, REPLY, success);
			break;

		case READ:
			val = readKey(m.key);
			if (val.size() > 0) {
				log->logReadSuccess(&(memberNode->addr), false, m.transID, m.key, val);
			} else {
				log->logReadFail(&(memberNode->addr), false, m.transID, m.key);
			}
			reply = new Message(m.transID, memberNode->addr, val);
			break;

		case UPDATE:
			success = updateKeyValue(m.key, m.value, m.replica);
			if (success) {
				log->logUpdateSuccess(&(memberNode->addr), false, m.transID, m.key, m.value);
			} else {
				log->logUpdateFail(&(memberNode->addr), false, m.transID, m.key, m.value);
			}
			reply = new Message(m.transID, memberNode->addr, REPLY, success);
			break;

		case DELETE:
			success = deletekey(m.key);
			if (success) {
				log->logDeleteSuccess(&(memberNode->addr), false, m.transID, m.key);
			} else {
				log->logDeleteFail(&(memberNode->addr), false, m.transID, m.key);
			}
			reply = new Message(m.transID, memberNode->addr, REPLY, success);
			break;
		
		case REPLY:
			iter = transSet.find(m.transID);
			if (iter != transSet.end()) {
				quorum = iter->second;
			} else {
				quorum = 0;
			}
			if (m.success) {
				quorum = quorum + 1;
			}
			transSet[m.transID] = quorum;
			break;

		case READREPLY:
			iter = transSet.find(m.transID);
			if (iter != transSet.end()) {
				quorum = iter->second;
			} else {
				quorum = 0;
			}
			if (m.value.size() > 0) {
				quorum = quorum + 1; //update
				if (quorum == 2) {
					map<int, Message>::iterator search = opSet.find(iter->first);
					if (search != opSet.end()) {
						log->logReadSuccess(&memberNode->addr, true, m.transID, search->second.key, m.value);
					}
				}
			}
			transSet[m.transID] = quorum;
			break;

		default:
			break;
		}
		if (reply != NULL) {
			// just send reply
			emulNet->ENsend(&memberNode->addr, &m.fromAddr, reply->toString());
		}
	}

	/*
	 * This function should also ensure all READ and UPDATE operation
	 * get QUORUM replies
	 */
	for (iter = transSet.begin(); iter != transSet.end(); ++iter)
	{
		map<int, Message>::iterator search = opSet.find(iter->first);
		if (search == opSet.end()) {
			// not found?
			continue;
		}
		Message * msg = &(search->second);
		if (iter->second < 2) {
			switch (msg->type)
			{
			case CREATE:
				log->logCreateFail(&memberNode->addr, true, msg->transID, msg->key, msg->value);
				break;

			case UPDATE:
				log->logUpdateFail(&memberNode->addr, true, msg->transID, msg->key, msg->value);
				break;

			case READ:
				log->logReadFail(&memberNode->addr, true, msg->transID, msg->key);
				break;

			case DELETE:
				log->logDeleteFail(&memberNode->addr, true, msg->transID, msg->key);
				break;
			
			default:
				break;
			}
		} else {
			switch (msg->type)
			{
			case CREATE:
				log->logCreateSuccess(&memberNode->addr, true, msg->transID, msg->key, msg->value);
				break;

			case UPDATE:
				log->logUpdateSuccess(&memberNode->addr, true, msg->transID, msg->key, msg->value);
				break;

			case READ: // already logged
				break;

			case DELETE:
				log->logDeleteSuccess(&memberNode->addr, true, msg->transID, msg->key);
				break;
			
			default:
				break;
			}
		}
		opSet.erase(iter->first);
	}
	
}

/**
 * FUNCTION NAME: findNodes
 *
 * DESCRIPTION: Find the replicas of the given keyfunction
 * 				This function is responsible for finding the replicas of a key
 */
vector<Node> MP2Node::findNodes(string key) {
	size_t pos = hashFunction(key);
	vector<Node> addr_vec;
	if (ring.size() >= 3) {
		// if pos <= min || pos > max, the leader is the min
		if (pos <= ring.at(0).getHashCode() || pos > ring.at(ring.size()-1).getHashCode()) {
			addr_vec.emplace_back(ring.at(0));
			addr_vec.emplace_back(ring.at(1));
			addr_vec.emplace_back(ring.at(2));
		}
		else {
			// go through the ring until pos <= node
			for (int i=1; i<ring.size(); i++){
				Node addr = ring.at(i);
				if (pos <= addr.getHashCode()) {
					addr_vec.emplace_back(addr);
					addr_vec.emplace_back(ring.at((i+1)%ring.size()));
					addr_vec.emplace_back(ring.at((i+2)%ring.size()));
					break;
				}
			}
		}
	}
	return addr_vec;
}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: Receive messages from EmulNet and push into the queue (mp2q)
 */
bool MP2Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), this->enqueueWrapper, NULL, 1, &(memberNode->mp2q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue of MP2Node
 */
int MP2Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}
/**
 * FUNCTION NAME: stabilizationProtocol
 *
 * DESCRIPTION: This runs the stabilization protocol in case of Node joins and leaves
 * 				It ensures that there always 3 copies of all keys in the DHT at all times
 * 				The function does the following:
 *				1) Ensures that there are three "CORRECT" replicas of all the keys in spite of failures and joins
 *				Note:- "CORRECT" replicas implies that every key is replicated in its two neighboring nodes in the ring
 */
void MP2Node::stabilizationProtocol() {
	/*
	 * Implement this
	 */
	// at least 3 nodes
	if (ring.size() < 3) {
		return;
	}
	Node mynode(memberNode->addr);
	int mypos = 0;
	int mycode, prevcode;
	vector<Node> prevs;
	vector<Node> afters;
	for (int i=0; i<ring.size(); i++){
		Node node = ring.at(i);
		if (node.getHashCode() == mynode.getHashCode()) {
			mypos = i;
			break;
		}
	}
	prevs.emplace_back(ring.at((mypos-2)%ring.size()));
	prevs.emplace_back(ring.at((mypos-1)%ring.size()));
	afters.emplace_back(ring.at((mypos+1)%ring.size()));
	afters.emplace_back(ring.at((mypos+2)%ring.size()));
	if (!hasMyReplicas.empty() && !haveReplicasOf.empty()) {
		if (prevs.at(1).getHashCode() != haveReplicasOf.at(1).getHashCode()
		|| afters.at(0).getHashCode() != hasMyReplicas.at(0).getHashCode()
		|| afters.at(1).getHashCode() != hasMyReplicas.at(1).getHashCode()) {
			// value range is changed or backup replica node changed, should replica to afters
			for (map<string, string>::const_iterator it = ht->hashTable.cbegin();
				it != ht->hashTable.cend(); ++it) {
				size_t code = hashFunction(it->first);
				if ((prevcode < mycode && prevcode < code && code <= mycode)
					|| (prevcode > mycode && (prevcode < code || code <= mycode))) {
					Message m(0, memberNode->addr, CREATE, it->first, it->second, TERTIARY);
					emulNet->ENsend(&memberNode->addr, &afters.at(0).nodeAddress, m.toString());
					emulNet->ENsend(&memberNode->addr, &afters.at(1).nodeAddress, m.toString());
				}
			}
		}
	}
	hasMyReplicas = afters;
	haveReplicasOf = prevs;
}
