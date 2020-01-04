
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <netdb.h>
#include <unistd.h>
#include <sys/mman.h>
#include <arpa/inet.h>
#include "uthash.h"
#include "hashing.h"
#include "dht.h"
#include "queue.h"
#include <time.h>


#define ACK 8
#define GET 4
#define SET 2
#define DEL 1
#define BACKLOG 10


#define LOOKUP 129
#define REPLY 130
//TODO: arrange new Befehl

#define STABILIZE 132
#define NOTIFY 136
#define JOIN 144

#define FINAL 1001 //TODO hier muss auch geändert werden.
#define HASH 1002 //



#define unknownPeer 0
#define thisPeer 1
#define nextPeer 2

#define YES 1
#define NO 0 

// For the Clients
unsigned int opt;				
unsigned int keyLen;			
unsigned int valueLen;			
unsigned char* key;				
unsigned char* value;

// For the Peers
int nodeID;
int prevID=-1;
int nextID=-1;
int newID;

int nodeIP;
int nextIP;
int prevIP;
int notifyIP;

unsigned int nodePort;
unsigned int prevPort;
unsigned int nextPort;
unsigned int notifyPort;

unsigned char hashID0[2]={0,0};

int actual_HashRequest_sent = YES;

unsigned char* hashRequest;
int hashSocket; // Socket to the sender of the Hash Request   

int main(int argc, char** argv){

    if (argc < 4) {
        printf("Not enough arguments\n");
        exit(1);
    }

    // For the Select function 
    int control=0;           				// What kind of Connector, Clients or Peers?
    fd_set master;    					// master file descriptor list
    fd_set read_fds;  					// temp file descriptor list for select()
    int fdmax;        					// maximum file descriptor number 		

    
    // Peer and Neighbor Info 
    if (argc>3) nodeID = atoi(argv[3]);
    
    nodePort = atoi(argv[2]);
    printf("node Port:%d\n", nodePort);

    //use a generic socket address to store everything
    struct sockaddr saddr;
    //cast generic socket to an inet socket
    struct sockaddr_in * saddr_in = (struct sockaddr_in *) &saddr;
    //Convert IP address into inet address stored in sockaddr
    inet_aton(argv[1], &(saddr_in->sin_addr));
    nodeIP = *(int*)&(saddr_in->sin_addr);
    printf("node IP:%d\n", nodeIP);

    /*-------------------------------------------- GET PEER INFO --------------------------------------------------*/
    struct addrinfo hints, *servinfo;
    int status;

    memset(&hints, 0, sizeof hints);    	   // hints is empty
    int listener, nextSocket, newSocketFD, firstPeerSocket, chosenPeerSocket, prevSocket=0, notifySocket, yes=1;
    struct sockaddr_storage addrInfo;    	   // connector's addresponses Info
    socklen_t addrSize;

    hints.ai_family = AF_INET;        	   	   // IPv4
    hints.ai_socktype = SOCK_STREAM;           // Stream listener
    hints.ai_flags = AI_PASSIVE;               // Use my IP


    // Get Info of the actual peer   
    status = getaddrinfo(NULL, argv[2], &hints, &servinfo);
    if (status != 0) {
        printf("getaddrinfo error: %s\n",gai_strerror(status));
        exit(1);
    }
    //--------------------------------------------------------------------------------
    if (argc==6) {    //Create Join to Chord-Ring
        

        //unsigned char* nullHashID = calloc(2, 1);   //create two bytes of memory
        unsigned char* joinRequest = createPeerRequest(hashID0,nodeID,nodeIP,nodePort,JOIN);
        // Connect to the known peer
        int knownPeerSocket = createConnection(argv[4], argv[5],NULL);

        if (send(knownPeerSocket,joinRequest,11,0) == -1) {
            perror("Error in sending\n");
        }
        printf("1st JOIN sent\n");
        //TODO just test it later: free(nullHashID);
    }
    //--------------------------------------------------------------------------------
    while(1) {
    	// Create the Listener Socket
    	listener = socket(servinfo->ai_family, servinfo->ai_socktype, servinfo->ai_protocol);
   		if(listener == -1) {
       	perror("Failed to create a listener\n");
       	continue;
    	}
        //so we can reuse address/port
        if (setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int)) == -1) {
            perror("setsockopt");
            exit(1);
        }
    	if (bind(listener, servinfo->ai_addr, servinfo->ai_addrlen) == -1) {
            close(listener);
            perror("Problems with binding\n");
            continue;
        }
        break; // sucessfully bind
    }

    freeaddrinfo(servinfo);
    printf("Peer %d: Binding to the listener\n", nodeID);

    /*---------------------------------------------------------------------------------------------------------------*/

    // Create Hash Table
    hashable *hTab = NULL; // Hash Table must be set to NULL at first 

    // LISTEN WITH THE LISTENER
    if (listen(listener, BACKLOG) == -1) {
        perror("Can't hear anything");
        exit(1);
    }

    FD_ZERO(&master);          // clear the master and temp sets
    FD_ZERO(&read_fds);	
    FD_SET(listener, &master); // add the listener to the master set
    fdmax = listener;		   // keep track of the biggest file descriptor

    struct requestSocketQueue* waitingSocketQueue = malloc(sizeof(struct requestSocketQueue*));
    waitingSocketQueue->head = NULL;
    
    while(1){

        read_fds = master;    // copy it
        if (select(fdmax+1, &read_fds, NULL, NULL, NULL) == -1) {
            perror("Select");
            exit(4);
        }

        // run through the existing connections looking for data to read
        for(int i = 0; i <= fdmax; i++) {
            if (FD_ISSET(i, &read_fds)) { // we got one!!
                if (i == listener) {    
                    // handle new connections
                    addrSize = sizeof(addrInfo);
                    newSocketFD = accept(listener,(struct sockaddr *)&addrInfo,&addrSize);
                    if (newSocketFD == -1) {
                        perror("Unacceptable");
                    } else {
                        FD_SET(newSocketFD, &master); // add to master set
                        if (newSocketFD > fdmax){     // keep track of the max
                            fdmax = newSocketFD;
                        }
                    }
                } else {
                    unsigned char *firstByte = malloc(1);
                    // handle data from a connector
                    if (is_in_the_queue(waitingSocketQueue, i) == NO) {
                        // Get the first Byte
                        int msglen = recv(i, firstByte, 1, 0);
                        if (msglen <= 0) {
                            perror("Error in receiving");
                            close(i);
                            FD_CLR(i, &master);
                        }
                        control = firstByteDecode(firstByte, &opt);
                        printf("Recv 1stByte=%d  ",control);
                    }

                    if (actual_HashRequest_sent == NO && control == HASH) {
                        enqueue(waitingSocketQueue, i, firstByte);
                        continue;
                    }

                    if (is_in_the_queue(waitingSocketQueue, i) == YES && actual_HashRequest_sent == YES) {
                        struct socketQueueElem *tmp = dequeue(waitingSocketQueue, i);
                        memcpy(firstByte, tmp->firstByte, 1);
                        free(tmp->firstByte);
                        free(tmp);
                        control = HASH;
                    }

                    if (control == HASH) {
                        printf("control == HASH\n");
                        //printf("Peer %d: received a Hash Request\n", nodeID);
                        hashRequest = getHashRequest(i, firstByte, &key, &value, &keyLen, &valueLen);
                        hashSocket = i;
                        actual_HashRequest_sent = NO;

                        unsigned char *hashKey;
                        if (keyLen == 1) {
                            hashKey = malloc(2);
                            hashKey[0] = key[0];
                            hashKey[1] = 0;
                        } else {
                            hashKey = key;
                        }

                        int hashValue = ringHashing(hashKey); //TODO consistent hashing
                        //printf("Peer %d: hashValue is %d\n", nodeID, hashValue);
                        if (checkPeer(nodeID, prevID, nextID, hashValue) ==
                            thisPeer) {        //TODO This peer is responsible for this Request
                            //printf("Peer %d: I'm responsible for the request\n", nodeID); //We determine each peer using ringHashing()
                            unsigned char *response;
                            unsigned int responseLen;
                            response = peerHashing(&hTab, opt, keyLen, valueLen, key, value, &responseLen);
                            if (send(i, response, responseLen, 0) == -1) {
                                perror("Error in sending\n");
                            }
                            //printf("peer %d: Hash request sent\n", nodeID);
                            actual_HashRequest_sent = YES;
                            close(hashSocket);
                            FD_CLR(i, &master);
                        } else if (checkPeer(nodeID, prevID, nextID, hashValue) ==
                                   nextPeer) {   //TODO Next peer is responsible for this Request
                            //printf("Peer %d: my next pal %d is responsible for the request\n", nodeID,nextID);

/*                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &nextIP, ipString, sizeof(ipString));
                            char portString[6];
                            uitoa(nextPort,portString);

                            nextSocket = createConnection(ipString,portString,NULL);
                            */
                            if (send(nextSocket, hashRequest, 7 + keyLen + valueLen, 0) == -1) {
                                perror("Error in sending\n");
                            }

                            FD_SET(nextSocket, &master); // add to master set
                            if (nextSocket > fdmax) {     // keep track of the max
                                fdmax = nextSocket;
                            }
                        } else {                                                               //TODO unknown Peer
                            //printf("Peer %d: I dunno but I'll ask my next pal %d\n", nodeID,nextID);                                          
                            unsigned char *peerRequest;
                            //create and send LOOKUP Request
                            //printf("Peer %d: my IP is %d\n", nodeID, nodeIP);
                            peerRequest = createPeerRequest(hashKey, nodeID, nodeIP, nodePort, LOOKUP);
/*
                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &nextIP, ipString, sizeof(ipString));
                            char portString[6];
                            uitoa(nextPort,portString);

                            nextSocket = createConnection(ipString,portString,NULL);
                            */
                            if (send(nextSocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                        }
                    } else if (control == LOOKUP) {
                        printf("control == LOOKUP\n");
                        //get full request
                        //printf("Peer %d: received a LOOKUP Request\n", nodeID);
                        unsigned char *peerRequest;
                        peerRequest = getPeerRequest(i, firstByte);
                        unsigned char *hashID = malloc(2);
                        memcpy(hashID, peerRequest + 1, 2);
                        int hashValue = ringHashing(hashID);
                        //printf("Peer %d: hashValue is %d\n", nodeID, hashValue);

                        // There won't be the case of thisPeer with LOOKUP

                        if (checkPeer(nodeID, prevID, nextID, hashValue) == nextPeer) {
                            //printf("Peer %d: my next pal %d is responsible for the request\n", nodeID,nextID);

                            int firstPeerIP;
                            memcpy(&firstPeerIP, peerRequest + 5, 4);
                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &firstPeerIP, ipString, sizeof(ipString));

                            //inet_ntop and uitoa=convert IP & Port to String
                            unsigned int firstPeerPort = 0;
                            rv_memcpy(&firstPeerPort, peerRequest + 9, 2);
                            char portString[6];
                            uitoa(firstPeerPort, portString);

/*                            // Get the next IP //not efficient. Changed to code-block below
                            status = getaddrinfo(argv[8], argv[9], &hints, &servinfo);
                            if (status != 0) {
                                printf("getaddrinfo error: %s\n",gai_strerror(status));
                                exit(1);
                            }    
                            struct sockaddr_in *ipv4 = (struct sockaddr_in*) servinfo->ai_addr;
                            nextIP = *(int*)(&ipv4->sin_addr); //////////// Where magic happen /////////////////
                            freeaddrinfo(servinfo);
                            // Got the next IP


                            //use a generic socket address to store everything
                            struct sockaddr saddr;
                            //cast generic socket to an inet socket
                            struct sockaddr_in * saddr_in = (struct sockaddr_in *) &saddr;
                            //Convert IP address into inet address stored in sockaddr
                            inet_aton(argv[8], &(saddr_in->sin_addr));
                            nextIP = *(int*)&(saddr_in->sin_addr);
*/
                            peerRequest = createPeerRequest(hashID, nextID, nextIP, nextPort, REPLY);
                            // Connect to the first peer
                            firstPeerSocket = createConnection(ipString, portString, NULL);

                            if (send(firstPeerSocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                            //TODO just test it later: free(hashID);
                        } else if (checkPeer(nodeID, prevID, nextID, hashValue) == unknownPeer) {
                            //printf("Peer %d: I dunno but I'll ask my next pal %d\n", nodeID,nextID);

/*                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &nextIP, ipString, sizeof(ipString));
                            char portString[6];
                            uitoa(nextPort,portString);

                            nextSocket = createConnection(ipString,portString,NULL);
*/
                            if (send(nextSocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                        }
                    } else if (control == REPLY) {
                        printf("control == REPLY\n");
                        //get full request
                        //printf("Peer %d: received a REPLY Request\n", nodeID);
                        unsigned char *peerRequest;
                        peerRequest = getPeerRequest(i, firstByte);

                        int chosenPeerIP;
                        memcpy(&chosenPeerIP, peerRequest + 5, 4);
                        char ipString[INET_ADDRSTRLEN];
                        inet_ntop(AF_INET, &chosenPeerIP, ipString, sizeof(ipString));
                        //printf("Peer %d: IP of the chosen One: %s\n",nodeID,ipString);

                        unsigned int chosenPeerPort = 0;
                        rv_memcpy(&chosenPeerPort, peerRequest + 9, 2);
                        char portString[6];
                        uitoa(chosenPeerPort, portString);
                        //printf("Peer %d: Port of the chosen One: %s\n",nodeID,portString);

                        //connect and send Hash Request to the chosen one
                        chosenPeerSocket = createConnection(ipString, portString, NULL);
                        if (send(chosenPeerSocket, hashRequest, 7 + keyLen + valueLen, 0) == -1) {
                            perror("Error in sending\n");
                        }

                        FD_SET(chosenPeerSocket, &master); // add to master set
                        if (chosenPeerSocket > fdmax) {     // keep track of the max
                            fdmax = chosenPeerSocket;
                        }

                    } else if (control == FINAL) {
                        printf("control == FINAL\n");
                        //printf("Peer %d: received a FINAL Request\n", nodeID);

                        unsigned char *finalResponse;
                        finalResponse = getHashRequest(i, firstByte, &key, &value, &keyLen, &valueLen);

                        //printf("Got the final Response\n");

                        if (send(hashSocket, finalResponse, 7 + keyLen + valueLen, 0) == -1) {
                            perror("Error in sending\n");
                        }

                        actual_HashRequest_sent = YES;
                        free(finalResponse);
                        close(hashSocket);
                        FD_CLR(hashSocket, &master);
                        close(i);
                        FD_CLR(i, &master);
//--------------------------------------------------------------------------------------------------------
                    } else if (control == JOIN) {
                        printf("control == JOIN\n");
                        //get full request
                        //printf("Peer %d: received a JOIN Request\n", nodeID);
                        unsigned char *peerRequest;
                        peerRequest = getPeerRequest(i, firstByte);
                        //unsigned char* nullHashID = calloc(2, 1);   //create two bytes of memory
                        memcpy(&newID, peerRequest + 3, 2);
                        newID=ntohs(newID);

                        if ((newID < nextID || prevID==-1) && newID < nodeID) { //then you are my new prev
                            prevID = newID;

                            //read my new prev IP & Port from recv
                            memcpy(&prevIP, peerRequest + 5, 4);
                            //prevIP=ntohl(prevIP);
                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &prevIP, ipString, sizeof(ipString));

                            //inet_ntop and uitoa=convert IP & Port to String
                            memcpy(&prevPort, peerRequest + 9, 2);
                            prevPort=ntohs(prevPort);
                            char portString[6];
                            uitoa(prevPort, portString);

                            //reply with notify
                            peerRequest = createPeerRequest(hashID0, prevID, prevIP, prevPort, NOTIFY);
                            // Connect to the new prev peer
                            prevSocket = createConnection(ipString, portString, NULL);

                            if (send(prevSocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                            printf("you are my new prev");
                        }

                        //case when it dont have prev or next
                        else if(nextID==-1 && newID>nodeID) {
                            nextID=newID;

                            //read my new prev IP & Port from recv
                            memcpy(&nextIP, peerRequest + 5, 4);
                            //nextIP=ntohl(nextIP);
                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &nextIP, ipString, sizeof(ipString));

                            //inet_ntop and uitoa=convert IP & Port to String
                            memcpy(&nextPort, peerRequest + 9, 2);
                            nextPort=ntohs(nextPort);
                            char portString[6];
                            uitoa(nextPort, portString);
                            printf("you are my first next.\n");
                            //NO reply with notify. Instead I will send stabilize to you now
                        }
                        /*else if(prevID==-1 && newID<nodeID) {
                            prevID=newID;
                        }*/


                        else { //you aren't my prev
                            //forward to next peer
/*
                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &nextIP, ipString, sizeof(ipString));

                            char portString[6];
                            uitoa(prevPort,portString);

                            nextSocket = createConnection(ipString,portString,NULL);
*/

                            if (send(nextSocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                        }
                    } else if (control == NOTIFY) {
                        printf("control == NOTIFY\n");
                        //get full request
                        unsigned char *peerRequest;
                        peerRequest = getPeerRequest(i, firstByte);

                        memcpy(&newID, peerRequest + 3, 2);
                        newID=ntohs(newID);
                        //if nodeID != newID, update nextID, nextIP, nextPort. Else do nothing
                        if (nodeID != newID) {
                            nextID = newID;
                            memcpy(&nextIP, peerRequest + 5, 4);
                            nextIP=ntohl(nextIP);printf("IP:%d  ", nextIP);
                            memcpy(&nextPort, peerRequest + 9, 2);
                            nextPort=ntohs(nextPort);
                        }
                        sleep(1);
                    } else if (control == STABILIZE) {
                        printf("control == STABILIZE\n");
                        //get full request
                        unsigned char *peerRequest;
                        peerRequest = getPeerRequest(i, firstByte);

                        memcpy(&newID, peerRequest + 3, 2);
                        newID=ntohs(newID);

                        //reply with notify.
                        //Use socket prevSocket if i reply to my prevID, otherwise CREATE new socket (notifySocket)
                        if (newID == prevID) {
                            if (prevSocket==0){ //if socket isnt exist yet, create one. Else use existing socket
                                //read my new prev IP & Port from recv
                                memcpy(&prevIP, peerRequest + 5, 4);
                                //prevIP=ntohl(prevIP);
                                char ipString[INET_ADDRSTRLEN];
                                inet_ntop(AF_INET, &prevIP, ipString, sizeof(ipString));
                                printf("Notif. IP:%d  %s  ",notifyIP, ipString);

                                //inet_ntop and uitoa=convert IP & Port to String
                                memcpy(&prevPort, peerRequest + 9, 2);
                                prevPort=ntohs(prevPort);
                                char portString[6];
                                uitoa(prevPort, portString);
                                printf("Notif. Port:%d  %s\n", notifyPort, portString);

                                prevSocket = createConnection(ipString, portString, NULL);
                            }


                            peerRequest = createPeerRequest(hashID0, prevID, prevIP, prevPort, NOTIFY);

                            if (send(prevSocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                        } else {
                            //read my new prev IP & Port from recv
                            memcpy(&notifyIP, peerRequest + 5, 4);
                            //notifyIP=ntohl(notifyIP);
                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &notifyIP, ipString, sizeof(ipString));
                            printf("Notif. IP:%d  %s  ",notifyIP, ipString);

                            //inet_ntop and uitoa=convert IP & Port to String
                            memcpy(&notifyPort, peerRequest + 9, 2);
                            notifyPort=ntohs(notifyPort);

                            char portString[6];
                            uitoa(notifyPort, portString);
                            printf("Notif. Port:%d  %s\n", notifyPort, portString);

                            //update prevID when necessary
                            if (newID > prevID) {
                                prevID = newID;
                                prevIP = notifyIP;
                                prevPort = notifyPort;
                            }

                            peerRequest = createPeerRequest(hashID0, prevID, prevIP, prevPort, NOTIFY);
                            // Connect to the new prev peer
                            notifySocket = createConnection(ipString, portString, NULL);

                            if (send(notifySocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                        }
                        sleep(1);
                    }


                    //send stabilize every 2 sec. Doesnt matter if(control ==..)
                    if (nextID!=-1){
                            printf("Stabilizing Now  ");
                            //create connection to known nextID
                            char ipString[INET_ADDRSTRLEN];
                            inet_ntop(AF_INET, &nextIP, ipString, sizeof(ipString));
                            printf("IP:%s  ", ipString);

                            char portString[6];
                            uitoa(nextPort, portString);
                            printf("Port:%s\n", portString);

                            unsigned char *peerRequest;
                            peerRequest = createPeerRequest(hashID0, nodeID, nodeIP, nodePort, STABILIZE);
                            nextSocket = createConnection(ipString, portString, NULL);
                            if (send(nextSocket, peerRequest, 11, 0) == -1) {
                                perror("Error in sending\n");
                            }
                        sleep(2);
                        }
                }
            }
        }        
    }
    return 0;
                      
}
