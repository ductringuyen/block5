#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/mman.h>
#include "uthash.h"
#include "hashing.h"
#include "dht.h"
#include <arpa/inet.h>  //////magical library//////
#include <time.h>


#define ACK 8
#define GET 4
#define SET 2
#define DEL 1
#define BACKLOG 10

#define LOOKUP 129
#define REPLY 130

#define STABILIZE 132
#define NOTIFY 136
#define JOIN 144

#define FINAL 1001 //TODO
#define HASH 1002

#define unknownPeer 0
#define thisPeer 1
#define nextPeer 2

typedef struct a{
    unsigned int id;
    char ip[16];
    unsigned int port;
}node;

node fingerTable[33];
node temp;
node predecessor;
node successor;
node self;

char targetIP[16];
int targetPort;
char predBuffer[256];	// find predecessor buffer, zum Beispiel, noch nicht modifiert with the codebase
char succBuffer[256];	// find successor buffer, zum Beispiel, noch nicht modifiert with the codebase

static int inBetween(int id, int start, int end, int inclusive)
{
    if( end > start)
    {
        return (((start < id) && (id < end)) || (inclusive && ((id == end))));
    }

    else
    {
        return ((start < id) || (id < end) || (inclusive && ((id == end))));
    }
}

int ringHashing(unsigned char* key) {
  int hashValue = 0;
  for (int i = 0; i < 2; i++) {
    hashValue += (int)(key[i]%80);
  }
  return hashValue;
};

int checkPeer(unsigned int nodeID, unsigned int prevID, unsigned int nextID, int hashValue) {
	if ((hashValue <= nodeID && hashValue > prevID) || (prevID > nodeID && hashValue > prevID) || (prevID > nodeID && hashValue < nodeID)) {
		return thisPeer;
	} else if ((hashValue <= nextID && hashValue > nodeID) || (nextID < nodeID && hashValue > nodeID) || (nextID < nodeID && hashValue < nextID)) {
		return nextPeer;
	}
	return unknownPeer;
}

//TODO: decode for join,notify,stabilize : DONE
int firstByteDecode(unsigned char* firstByte, unsigned int* opt) {
	if (*firstByte == 129) {
		return LOOKUP;
	}
	else if (*firstByte == 130) {
		return REPLY;
	}
    else if (*firstByte == 132) {
        return STABILIZE;
    }
    else if (*firstByte == 136) {
        return NOTIFY;
    }
    else if (*firstByte == 144) {
        return JOIN;
    }
	else if (*firstByte < 8){
		*opt = (int) *firstByte;
		return HASH;
	}
	return FINAL;	
}

void hashHeaderAnalize(unsigned char* header, unsigned int* keyLen, unsigned int* valueLen) {
	*keyLen = (header[0]<<8) + header[1];
	*valueLen = (header[2]<<24) + (header[3]<<16) + (header[4]<<8) + header[5];
}

unsigned char* getHashRequest(int socketfd, unsigned char* firstByte, unsigned char** key, unsigned char** value, unsigned int* keyLen, unsigned int* valueLen){
	unsigned char* header;
	unsigned char* data;
	unsigned char* request;
  int written = 0;
  int msglen = 0;

	// Get the Header
    header = malloc(6);	
    msglen = recv(socketfd, header, 6, 0);
    if (msglen == -1) {
        perror("Error in receiving\n");
        exit(1);
    }
    hashHeaderAnalize(header, keyLen, valueLen); //TODO beter use ntoh. But not so important

    // Get the full equest
    data = malloc(*keyLen + *valueLen);
    while(1) {
       	msglen = recv(socketfd,data + written, *keyLen+*valueLen - written, 0);
       	if (msglen == -1) {
           	continue;
       	}
       	written += msglen;
       	if (written == *keyLen + *valueLen) break;	
    }
    request = malloc(7 + *keyLen + *valueLen);
    memcpy(request,firstByte,1);
    memcpy(request+1,header,6);
    memcpy(request+7,data,*valueLen+*keyLen);
    *key = malloc(*keyLen);
    memcpy(*key,request+7,*keyLen);
    int opt = *firstByte;
    if (opt == SET) {
        *value = malloc(*valueLen);
        memcpy(*value,request+7+*keyLen,*valueLen);
    }
    free(header);
    free(data);
    free(firstByte);
    return request;
}

unsigned char* peerHashing(hashable** hTab, unsigned int opt, unsigned int keyLen, unsigned int valueLen, unsigned char* key, unsigned char* value, unsigned int* responseLen) {
	unsigned char* response;
	unsigned int resLen;

	if (opt == SET) {
            printf("SET\n");
            resLen = 7;
           	response = calloc(resLen,1);
            response[0] = ACK + SET;			     // set Ack bit of the response
           	set(hTab, key, value, keyLen, valueLen);
        } 
    else if (opt == GET) {
        printf("GET\n");
        hashable *hashElem = get(hTab, key, keyLen);
       	if (hashElem == NULL) {
            resLen = 7;
            response = calloc(resLen,1);
            response[0] = GET;            // set Ack bit of the response
        } else {
            valueLen = hashElem->valueLen;
            resLen = 7 + hashElem->keyLen + hashElem->valueLen;
            response = malloc(resLen);
            response[0] = ACK + GET;            // set Ack bit of the response
            keyLen=htons(keyLen);
            valueLen=htonl(valueLen);
            memcpy(response + 1, &keyLen, 2);
            memcpy(response + 3, &valueLen, 4);
            memcpy(response + 7,hashElem->hashKey,hashElem->keyLen);
            memcpy(response + 7 + hashElem->keyLen,hashElem->hashValue,hashElem->valueLen);
           }
        }
    else if (opt == DEL) {
           printf("DELETE\n"); 
           resLen = 7;
           response = (unsigned char*) calloc(resLen,1);
           response[0] = ACK + DEL;			// set Ack bit of the response
           delete(hTab, key, keyLen);
    }
    *responseLen = resLen;
    return response;
}

unsigned char* createPeerRequest(unsigned char* hashID, unsigned int nodeID, unsigned int nodeIP, unsigned int nodePort, int operation) {
	unsigned char *request = malloc(11);
	*request = operation;printf("Building Req:%d  ", operation);
	printf("ID:%d  ", nodeID);printf("IP:%d  ", nodeIP);printf("Port:%d\n", nodePort);
    nodeID=htons(nodeID);
    //nodeIP=htonl(nodeIP);printf("CPR IP:%d\n", nodeIP);
    nodePort=htons(nodePort);
	memcpy(request+1,hashID,2);
	memcpy(request+3,&nodeID,2);
	memcpy(request+5,&nodeIP,4);
	memcpy(request+9,&nodePort,2);
	return request;
}

/*//TODO: create finger table
void create_finger_table(char *my_addr){
    int my_pos=0;
    double N = floor(log(N));

}*/

unsigned char* getPeerRequest(int socketfd, unsigned char* firstByte) {
	unsigned char* request = malloc(11);
	unsigned char* data = malloc(10);
	int msglen = recv(socketfd,data, 10, 0);
    if (msglen == -1) {
       	perror("Error in receiving\n");
       	exit(0);
    }
    memcpy(request,firstByte,1);
    memcpy(request+1,data,10);
    free(data);
    return request;
}

char* uitoa(unsigned int num, char *str) {
  sprintf(str, "%u", num);
  return str;
}

int createConnection(char* addr, char* port) {
    struct addrinfo hints, *servinfo;
    int status, yes=1;

    
    memset(&hints, 0, sizeof hints);          // hints is empty 
    int Socket;
    struct sockaddr_storage addrInfo;         // connector's addresponses Info
    socklen_t addrSize;

    hints.ai_family = AF_INET;                 // IPv4
    hints.ai_socktype = SOCK_STREAM;           // Stream listener
    hints.ai_flags = AI_PASSIVE;               // Use my IP

    status = getaddrinfo(addr, port, &hints, &servinfo);
    if (status != 0) {
        printf("getaddrinfo error: %s\n",gai_strerror(status));
        exit(1);
    }

    while(1) {
        Socket = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
        if(Socket == -1) {
            perror("Failed to create a Socket");
            continue;
        }
/*        if (setsockopt(Socket, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
            perror("setsockopt");
            exit(1);
        }*/
        // IF SOCKET IS CREATED, TRY TO CONNECT TO THE SERVER
        if (connect(Socket, servinfo->ai_addr, servinfo->ai_addrlen) == -1) {
            close(Socket);
            perror("Problems with creating connection");
            continue;
        }
        break; // sucessfully connect
    }

    freeaddrinfo(servinfo);
    return Socket;
}

int power(int x, unsigned int y) {
    if (y == 0)        return 1;
    else if (y%2 == 0) return power(x, y/2)*power(x, y/2);
    else               return x*power(x, y/2)*power(x, y/2);
}