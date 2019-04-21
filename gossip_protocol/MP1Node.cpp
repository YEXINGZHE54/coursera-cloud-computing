/**********************************
 * FILE NAME: MP1Node.cpp
 *
 * DESCRIPTION: Membership protocol run by this Node.
 * 				Definition of MP1Node class functions.
 **********************************/

#include <chrono>
#include "MP1Node.h"

/*
 * Note: You can change/add any functions in MP1Node.{h,cpp}
 */

/**
 * Overloaded Constructor of the MP1Node class
 * You can add new members to the class if you think it
 * is necessary for your logic to work
 */
MP1Node::MP1Node(Member *member, Params *params, EmulNet *emul, Log *log, Address *address) {
	for( int i = 0; i < 6; i++ ) {
		NULLADDR[i] = 0;
	}
	this->memberNode = member;
	this->emulNet = emul;
	this->log = log;
	this->par = params;
	this->memberNode->addr = *address;
}

/**
 * Destructor of the MP1Node class
 */
MP1Node::~MP1Node() {}

/**
 * FUNCTION NAME: recvLoop
 *
 * DESCRIPTION: This function receives message from the network and pushes into the queue
 * 				This function is called by a node to receive messages currently waiting for it
 */
int MP1Node::recvLoop() {
    if ( memberNode->bFailed ) {
    	return false;
    }
    else {
    	return emulNet->ENrecv(&(memberNode->addr), enqueueWrapper, NULL, 1, &(memberNode->mp1q));
    }
}

/**
 * FUNCTION NAME: enqueueWrapper
 *
 * DESCRIPTION: Enqueue the message from Emulnet into the queue
 */
int MP1Node::enqueueWrapper(void *env, char *buff, int size) {
	Queue q;
	return q.enqueue((queue<q_elt> *)env, (void *)buff, size);
}

/**
 * FUNCTION NAME: nodeStart
 *
 * DESCRIPTION: This function bootstraps the node
 * 				All initializations routines for a member.
 * 				Called by the application layer.
 */
void MP1Node::nodeStart(char *servaddrstr, short servport) {
    Address joinaddr;
    joinaddr = getJoinAddress();

    // Self booting routines
    if( initThisNode(&joinaddr) == -1 ) {
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "init_thisnode failed. Exit.");
#endif
        exit(1);
    }

    if( !introduceSelfToGroup(&joinaddr) ) {
        finishUpThisNode();
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Unable to join self to group. Exiting.");
#endif
        exit(1);
    }

    return;
}

/**
 * FUNCTION NAME: initThisNode
 *
 * DESCRIPTION: Find out who I am and start up
 */
int MP1Node::initThisNode(Address *joinaddr) {
	/*
	 * This function is partially implemented and may require changes
	 */
	int id = *(int*)(&memberNode->addr.addr);
	int port = *(short*)(&memberNode->addr.addr[4]);

	memberNode->bFailed = false;
	memberNode->inited = true;
	memberNode->inGroup = false;
    // node is up!
	memberNode->nnb = 0;
	memberNode->heartbeat = 0;
	memberNode->pingCounter = TFAIL;
	memberNode->timeOutCounter = -1;
    initMemberListTable(memberNode);

    return 0;
}

/**
 * FUNCTION NAME: introduceSelfToGroup
 *
 * DESCRIPTION: Join the distributed system
 */
int MP1Node::introduceSelfToGroup(Address *joinaddr) {
	MessageHdr *msg;
#ifdef DEBUGLOG
    static char s[1024];
#endif

    if ( 0 == memcmp((char *)&(memberNode->addr.addr), (char *)&(joinaddr->addr), sizeof(memberNode->addr.addr))) {
        // I am the group booter (first process to join the group). Boot up the group
#ifdef DEBUGLOG
        log->LOG(&memberNode->addr, "Starting up group...");
#endif
        memberNode->inGroup = true;
    }
    else {
#ifdef DEBUGLOG
        sprintf(s, "Trying to join...");
        log->LOG(&memberNode->addr, s);
#endif

        msg = createJoinRequest();
        // send JOINREQ message to introducer member
        emulNet->ENsend(&memberNode->addr, joinaddr, (char *)msg, sizeofJoinRequest());
        free(msg);
    }
    int id;
    short port;
    id = *(int *)memberNode->addr.addr;
    port = *(short *)&(memberNode->addr.addr[4]);
    MemberListEntry entry(id, port, 0, par->getcurrtime());
    memberNode->memberList.push_back(entry);
    notifyAdd(id, port);
    return 1;

}

/**
 * FUNCTION NAME: finishUpThisNode
 *
 * DESCRIPTION: Wind up this node and clean up state
 */
int MP1Node::finishUpThisNode(){
   /*
    * Your code goes here
    */
   memberNode->inGroup = false;
}

/**
 * FUNCTION NAME: nodeLoop
 *
 * DESCRIPTION: Executed periodically at each member
 * 				Check your messages in queue and perform membership protocol duties
 */
void MP1Node::nodeLoop() {
    if (memberNode->bFailed) {
    	return;
    }

    // Check my messages
    checkMessages();

    // Wait until you're in the group...
    if( !memberNode->inGroup ) {
    	return;
    }

    // ...then jump in and share your responsibilites!
    nodeLoopOps();

    return;
}

/**
 * FUNCTION NAME: checkMessages
 *
 * DESCRIPTION: Check messages in the queue and call the respective message handler
 */
void MP1Node::checkMessages() {
    void *ptr;
    int size;

    // Pop waiting messages from memberNode's mp1q
    while ( !memberNode->mp1q.empty() ) {
    	ptr = memberNode->mp1q.front().elt;
    	size = memberNode->mp1q.front().size;
    	memberNode->mp1q.pop();
    	recvCallBack((void *)memberNode, (char *)ptr, size);
    }
    return;
}

/**
 * FUNCTION NAME: recvCallBack
 *
 * DESCRIPTION: Message handler for different message types
 */
bool MP1Node::recvCallBack(void *env, char *data, int size ) {
	/*
	 * Your code goes here
	 */
    MessageHdr *msg;
    MessageHdr *msgToSend;
    vector<MemberListEntry> diff;
    Address source;
    long heartbeat;
    int id = 0;
	short port;
    bool found = false;
    long now = nowtime();
    msg = (MessageHdr *)data;
    switch (msg->msgType)
    {
        case HEARTBEATE:
            memcpy(&source.addr, (char *)(msg+1), sizeof(source.addr));
            memcpy(&heartbeat, (char *)(msg+1) + 1 + sizeof(source.addr), sizeof(long));
            memcpy(&id, &source.addr[0], sizeof(int));
		    memcpy(&port, &source.addr[4], sizeof(short));
            for (auto & it : memberNode->memberList) {
                if (it.getid() == id && it.getport() == port) {
                    if (heartbeat > it.heartbeat) {
                        it.timestamp = now;
                        it.heartbeat = heartbeat;
                    }
                    found = true;
                    break;
                }
            }
            if (!found) {
                MemberListEntry entry(id, port, heartbeat, now);
                memberNode->memberList.push_back(entry);
                notifyAdd(id, port);
            }
            diff = parseNodes((char *)(msg+1) + 1 + sizeof(source.addr) + sizeof(long));
            msgToSend = createHeartBeatReply(diff);
            emulNet->ENsend(&memberNode->addr, &source, (char *)msgToSend, sizeofHeartBeatReply(diff));
            free(msgToSend);
            break;

        case HEARTBEATEREPLY:
            parseNodes((char *)(msg+1));
            break;

        default:
            cout<<"invalid msgType received:"<<msg->msgType<<endl;
            break;
    }
    free(msg);
}

/**
 * FUNCTION NAME: nodeLoopOps
 *
 * DESCRIPTION: Check if any node hasn't responded within a timeout period and then delete
 * 				the nodes
 * 				Propagate your membership list
 */
void MP1Node::nodeLoopOps() {

	/*
	 * Your code goes here
	 */
    Address addr;
    int id;
    short port;
    id = *(int *)(memberNode->addr.addr);
    port = *(short *)&(memberNode->addr.addr[4]);
    // cleanup
    memberNode->heartbeat++;
    long now = nowtime();
    std::vector<MemberListEntry> watches;
    for (std::vector<MemberListEntry>::iterator it = memberNode->memberList.begin();
        it != memberNode->memberList.end(); ++it) {
        if (it->id == id && it->port == port) {
            it->heartbeat = memberNode->heartbeat;
            it->timestamp = now;
        }
        if (it->gettimestamp() < now - TREMOVE) {
            notifyDel(it->getid(), it->getport());
        } else {
            watches.push_back(*it);
        }
    }
    memberNode->memberList = watches;

    std::vector<MemberListEntry> alives;
    for (auto & it : watches) {
        if (it.timestamp > now - TFAIL) {
            alives.push_back(it);
        }
    }
  
    // pick multi and propagate state
    for(auto & ele : memberNode->memberList)
    {
        memcpy(&addr.addr[0], &ele.id, sizeof(ele.id));
        memcpy(&addr.addr[4], &ele.port, sizeof(ele.port));
        MessageHdr * msg = createHeartBeatRequest(alives);
        emulNet->ENsend(&memberNode->addr, &addr, (char *)msg, sizeofHeartBeatRequest(alives));
        free(msg);
    }
    return;
}

/**
 * FUNCTION NAME: isNullAddress
 *
 * DESCRIPTION: Function checks if the address is NULL
 */
int MP1Node::isNullAddress(Address *addr) {
	return (memcmp(addr->addr, NULLADDR, 6) == 0 ? 1 : 0);
}

/**
 * FUNCTION NAME: getJoinAddress
 *
 * DESCRIPTION: Returns the Address of the coordinator
 */
Address MP1Node::getJoinAddress() {
    Address joinaddr;

    memset(&joinaddr, 0, sizeof(Address));
    *(int *)(&joinaddr.addr) = 1;
    *(short *)(&joinaddr.addr[4]) = 0;

    return joinaddr;
}

/**
 * FUNCTION NAME: initMemberListTable
 *
 * DESCRIPTION: Initialize the membership list
 */
void MP1Node::initMemberListTable(Member *memberNode) {
	memberNode->memberList.clear();
}

/**
 * FUNCTION NAME: printAddress
 *
 * DESCRIPTION: Print the Address
 */
void MP1Node::printAddress(Address *addr)
{
    printf("%d.%d.%d.%d:%d \n",  addr->addr[0],addr->addr[1],addr->addr[2],
                                                       addr->addr[3], *(short*)&addr->addr[4]) ;    
}

size_t MP1Node::sizeofJoinRequest() {
    return sizeofHeartBeatRequest(memberNode->memberList);
}

MessageHdr * MP1Node::createJoinRequest() {
    return createHeartBeatRequest(memberNode->memberList);
}

MessageHdr * MP1Node::createHeartBeatRequest(vector<MemberListEntry> nodes) {
    MessageHdr * msg;
    char * buf;
    int nodesize = nodes.size();
    size_t msgsize = sizeofHeartBeatRequest(nodes);
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create heartbeat message: format of data is {MessageHdr Addr 1char heartbeat intsize MemberListEntry}
    msg->msgType = HEARTBEATE;
    memcpy((char *)(msg+1), &memberNode->addr.addr, sizeof(memberNode->addr.addr));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr), &memberNode->heartbeat, sizeof(long));
    memcpy((char *)(msg+1) + 1 + sizeof(memberNode->addr.addr) + sizeof(long), &nodesize, sizeof(int));
    buf = (char *)(msg+1) + 1 + sizeof(memberNode->addr.addr) + sizeof(long) + sizeof(int);
    for (auto & it : nodes) {
        memcpy(buf, &it.id, sizeof(it.id));
        buf = buf + sizeof(it.id);
        memcpy(buf, &it.port, sizeof(it.port));
        buf = buf + sizeof(it.port);
        memcpy(buf, &it.heartbeat, sizeof(it.heartbeat));
        buf = buf + sizeof(it.heartbeat);
        memcpy(buf, &it.timestamp, sizeof(it.timestamp));
        buf = buf + sizeof(it.timestamp);
    }
    return msg;
};
size_t MP1Node::sizeofHeartBeatRequest(vector<MemberListEntry> nodes) {
    size_t elesize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    return sizeof(MessageHdr) + sizeof(memberNode->addr.addr) + sizeof(long) + 1 + sizeof(int) + nodes.size() * elesize;
};
MessageHdr * MP1Node::createHeartBeatReply(vector<MemberListEntry> nodes){
    MessageHdr * msg;
    char * buf;
    int nodesize = nodes.size();
    size_t msgsize = sizeofHeartBeatReply(nodes);
    msg = (MessageHdr *) malloc(msgsize * sizeof(char));

    // create heartbeat reply message: format of data is {MessageHdr intsize MemberListEntry}
    msg->msgType = HEARTBEATEREPLY;
    memcpy((char *)(msg+1), &nodesize, sizeof(int));
    buf = (char *)(msg+1) + sizeof(int);
    for (auto & it : nodes) {
        memcpy(buf, &it.id, sizeof(it.id));
        buf = buf + sizeof(it.id);
        memcpy(buf, &it.port, sizeof(it.port));
        buf = buf + sizeof(it.port);
        memcpy(buf, &it.heartbeat, sizeof(it.heartbeat));
        buf = buf + sizeof(it.heartbeat);
        memcpy(buf, &it.timestamp, sizeof(it.timestamp));
        buf = buf + sizeof(it.timestamp);
    }
    return msg;
};
size_t MP1Node::sizeofHeartBeatReply(vector<MemberListEntry> nodes) {
    size_t elesize = sizeof(int) + sizeof(short) + sizeof(long) + sizeof(long);
    return sizeof(MessageHdr) + sizeof(int) + nodes.size() * elesize;
};
vector<MemberListEntry> MP1Node::parseNodes(char * data) {
    int elesize;
    char * buf = data + sizeof(int);
    int id;
    short port;
    long heartbeat;
    long timestamp;
    bool found;
    std::vector<MemberListEntry> alives;
    long now = nowtime();
    memberNode->inGroup = true;
    memcpy(&elesize, data, sizeof(elesize));
    for(size_t i = 0; i < elesize; i++, found = false)
    {
        memcpy(&id, buf, sizeof(id));
        buf = buf + sizeof(id);
        memcpy(&port, buf, sizeof(port));
        buf = buf + sizeof(port);
        memcpy(&heartbeat, buf, sizeof(heartbeat));
        buf = buf + sizeof(heartbeat);
        memcpy(&timestamp, buf, sizeof(timestamp));
        buf = buf + sizeof(timestamp);
        for (auto & it : memberNode->memberList) {
            if (it.getid() == id && it.getport() == port) {
                if (heartbeat > it.heartbeat) {
                    it.timestamp = now;
                    it.heartbeat = heartbeat;
                }
                found = true;
                break;
            }
        }
        if (!found) {
            MemberListEntry entry(id, port, heartbeat, now);
            memberNode->memberList.push_back(entry);
            notifyAdd(id, port);
        }
    }
    for (auto & it : memberNode->memberList) {
        if (it.timestamp > now - TFAIL) {
            alives.push_back(it);
        }
    }
    return alives;
};
void MP1Node::notifyAdd(int id, short port) {
    Address dest;
    memcpy(&dest.addr[0], &id, sizeof(id));
    memcpy(&dest.addr[4], &port, sizeof(port));
    log->logNodeAdd(&memberNode->addr, &dest);
}
void MP1Node::notifyDel(int id, short port) {
    Address dest;
    memcpy(&dest.addr[0], &id, sizeof(id));
    memcpy(&dest.addr[4], &port, sizeof(port));
    log->logNodeRemove(&memberNode->addr, &dest);
}