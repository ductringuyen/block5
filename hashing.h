typedef struct s_hashable
{
	void* hashKey;
	void* hashValue;
	unsigned int keyLen;
	unsigned int valueLen;
	UT_hash_handle hh;
} hashable ;

void* keyProcessing(hashable* hTab, void* key, unsigned int kl);

hashable* get(hashable** hTab, void* key, unsigned int kl);

void set(hashable** hTab, void* key, void* value, unsigned int kl, unsigned int vl); 

void delete(hashable** hTab, void* key, unsigned int kl);






