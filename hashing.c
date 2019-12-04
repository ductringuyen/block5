#include <stdio.h>
#include <stdlib.h>
#include "uthash.h"
#include "hashing.h"

void* keyProcessing(hashable* hTab, void* key, unsigned int kl) {
	void* modKey;
	if (key == NULL) return NULL;
	for (hashable* s = hTab; s != NULL; s = s->hh.next) {
		if ((s->keyLen == kl) && (memcmp(s->hashKey,key,kl)==0)) {
			modKey = s->hashKey;
			return modKey;	
		} 
	}
	modKey = malloc(kl);
	memcpy(modKey,key,kl);
	return modKey;
}

hashable* get(hashable** hTab, void* key, unsigned int kl) {
	hashable *hashElem;
	void* modKey = keyProcessing(*hTab,key,kl);
	HASH_FIND_PTR(*hTab,&modKey,hashElem);
	return hashElem;
}

void set(hashable** hTab, void* key, void* value, unsigned int kl, unsigned int vl) {
	hashable *hashElem;
	void* modKey = keyProcessing(*hTab,key,kl);
	HASH_FIND_PTR(*hTab,&modKey,hashElem);
	hashable *hashTest;
	if (hashElem == NULL) {
		hashElem = malloc(sizeof(hashable));
	} else {
		delete(hTab,key,kl);
	}
	hashElem->hashValue = malloc(vl);
	hashElem->keyLen = kl;
	hashElem->valueLen = vl;
	hashElem->hashKey = modKey;
	memcpy(hashElem->hashValue,value,vl);
	HASH_ADD_PTR(*hTab,hashKey,hashElem);
}

void delete(hashable** hTab, void* key, unsigned int kl) {
	hashable *hashElem = malloc(sizeof(hashable));
	void* modKey = keyProcessing(*hTab,key,kl);
	HASH_FIND_PTR(*hTab,&modKey,hashElem);
	if (hashElem != NULL) HASH_DEL(*hTab,hashElem);
} 



