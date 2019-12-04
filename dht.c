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


#define ACK 8
#define GET 4
#define SET 2
#define DEL 1
#define BACKLOG 10

#define LOOKUP 129
#define REPLY 130

#define STABILIZE 131
#define NOTIFY 132
#define JOIN 133

#define FINAL 131
#define HASH 132

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


//TODO: check if ID unique
int checkID(){

}

//TODO: peers as clients
void client(char buffer[256])
{
    int sockfd, n;
    struct sockaddr_in serverAddr;

    sockfd = socket(AF_INET, SOCK_STREAM,0);
    if(sockfd < 0)
    {
        fprintf(stderr,"Error opening socket\n");
        exit(1);
    }

    bzero((char *) &serverAddr, sizeof(serverAddr));
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(targetPort);
    serverAddr.sin_addr.s_addr = inet_addr(targetIP);  //////////Where new magic happen///////
    if(connect(sockfd,(struct sockaddr *) &serverAddr,sizeof(serverAddr)) < 0)
    {
        fprintf(stderr,"CLIENT: ERROR, Host with ip => [%s] and port => [%d] Network Error.\n",targetIP, targetPort);
        exit(1);
    }

    n = write(sockfd,buffer, 256);


    if (n < 0)
    {
        fprintf(stderr,"CLIENT: ERROR writing to socket\n");
    }

    bzero(buffer,256);
    n = read(sockfd,buffer,255);

    if (n < 0)
        fprintf(stderr, "CLIENT: ERROR reading from socket");
    close(sockfd);
    return;

}
//TODO: stabilize
void stabilize(){
    node x;
    while(1){
        if(self.id != successor.id || self.id != fingerTable[1].id){
            strcpy(targetIP, successor.ip);
            targetPort = successor.port;
            printf("STABILIZE: Target IP %s and port %d\nsuccessor ip %s and successor port %d \n",targetIP,targetPort, successor.ip, successor.port);
            for(int i=0; i<255;++i){
                predBuffer[i] = '\0';
            }
            strcpy(predBuffer, "findPredecessor");
            client(predBuffer);

            char arr[20];
            int i = 0;
            while(predBuffer[i] != ' ')
            {
                arr[i] = predBuffer[i];i++;
            }
            arr[i] = '\0';
            x.id = atoi(arr);

            for(int i=0; i<255;++i) {
                predBuffer[i] = '\0';
            }
            if(x.port != 0 && inBetween(x.id,self.id,successor.id,0))
            {
                successor.id = x.id;
                fingerTable[1].id = x.id;
                strcpy(successor.ip, x.ip);
                strcpy(fingerTable[1].ip,x.ip);
                successor.port = x.port;
                fingerTable[1].port = x.port;
                printf("STABILIZE: successor updated to %s:%d\n",successor.ip,successor.port);
            }
            //TODO:call for notify

        }
        }
    }

//TODO: notify
int notify(node x){
    if(self.id == x.id) return;
    if((predecessor.port == 0) ||(inBetween(x.id, predecessor.id,self.id,0))){
        predecessor.id = x.id;
        strncpy(predecessor.ip,x.ip,16);
        predecessor.port = x.port;
        printf("NOTIFY: Setting Predecessor to %s:%d\n", predecessor.ip, predecessor.port);
        fflush(stdout);
    }
    return NOTIFY;
}
//TODO:join
void join(){

}

int checkPeer(int nodeID, int prevID, int nextID, int hashValue) {
	if ((hashValue <= nodeID && hashValue > prevID) || (prevID > nodeID && hashValue > prevID) || (prevID > nodeID && hashValue < nodeID)) {
		return thisPeer;
	} else if ((hashValue <= nextID && hashValue > nodeID) || (nextID < nodeID && hashValue > nodeID) || (nextID < nodeID && hashValue < nextID)) {
		return nextPeer;
	}
	return unknownPeer;
}

//TODO: decode for join,notify,stabilize
int firstByteDecode(unsigned char* firstByte, unsigned int* opt) {
	if (*firstByte == 129) {
		return LOOKUP;
	}
	else if (*firstByte == 130) {
		return REPLY;
	}
	else if (*firstByte < 8){
		*opt = (int) *firstByte;
		return HASH;
	}
	return FINAL;	
}

void rv_memcpy(void* dst, void* src, unsigned int len) {
  unsigned char* dstByte = (unsigned char*) dst;
  unsigned char* srcByte = (unsigned char*) src;
  for (int i = 0; i < len; i++) {
    dstByte[i] = srcByte[len-1-i];
  }
}

void hashHeaderAnalize(unsigned char* header, unsigned int* keyLen, unsigned int* valueLen) {
	*keyLen = 256*(header[0]) + header[1];
	*valueLen = 16777216*(header[2]) + 65536*(header[3]) + 256*(header[4]) + header[5];
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
    hashHeaderAnalize(header, keyLen, valueLen);

    // Get the full equest
    data = malloc(*keyLen + *valueLen);
    while(1) {
       	msglen = recv(socketfd,data + written, 512, 0);
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
            response[0] = ACK + GET;            // set Ack bit of the response
        } else {
            valueLen = hashElem->valueLen;
            resLen = 7 + hashElem->keyLen + hashElem->valueLen;
            response = malloc(resLen);
            response[0] = ACK + GET;            // set Ack bit of the response
            rv_memcpy(response + 1, &keyLen, 2);
            rv_memcpy(response + 3, &valueLen, 4);
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
	*request = operation;
	memcpy(request+1,hashID,2);
	rv_memcpy(request+3,&nodeID,2);
	memcpy(request+5,&nodeIP,4);
	rv_memcpy(request+9,&nodePort,2);
	return request;
}

//TODO: create finger table
void create_finger_table(char *my_addr){
    int my_pos=0;
    double N = floor(log(N));

}

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

char* itoa(int num, char *str) {
  sprintf(str, "%d", num);
  return str;
}

int createConnection(char* addr, char* port, int* IP) {
    struct addrinfo hints, *servinfo;
    int status;

    
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
            perror("Failed to create a Socket\n");
            continue;
        }
        // IF SOCKET IS CREATED, TRY TO CONNECT TO THE SERVER
        if (connect(Socket, servinfo->ai_addr, servinfo->ai_addrlen) == -1) {
            close(Socket);
            perror("Problems with creating connection\n");
            continue;
        }
        break; // sucessfully connect
    }
    // Get IP as a number
    if (IP != NULL) {
      struct sockaddr_in *ipv4 = (struct sockaddr_in*) servinfo->ai_addr;
      *IP = *(int*)(&ipv4->sin_addr);
    }
    freeaddrinfo(servinfo);
    return Socket;
}
        
