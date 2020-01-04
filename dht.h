int ringHashing(unsigned char* key);

int checkPeer(unsigned int nodeID, unsigned int prevID, unsigned int nextID, int hashValue);

int firstByteDecode(unsigned char* firstByte, unsigned int* opt);

void rv_memcpy(void* dst, void* src, unsigned int len);

void hashHeaderAnalize(unsigned char* header, unsigned int* keyLen, unsigned int* valueLen);

unsigned char* getHashRequest(int socketfd, unsigned char* firstByte, unsigned char** key,unsigned char** value,unsigned int* keyLen,unsigned int* valueLen);

unsigned char* peerHashing(hashable** hTab,unsigned int opt, unsigned int keyLen, unsigned int valueLen, unsigned char* key, unsigned char* value, unsigned int* responseLen);

unsigned char* createPeerRequest(unsigned char* hashID, unsigned int nodeID, unsigned int nodeIP, unsigned int nodePort, int operation);

unsigned char* getPeerRequest(int socketfd, unsigned char* firstByte);

char* uitoa(unsigned int num, char *str);

int createConnection(char* addr, char* port, int* IP);