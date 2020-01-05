#include <stdio.h>
#include <stdlib.h>
#include<string.h> 
#include "queue.h"

int is_in_the_queue(struct requestSocketQueue* queue, int socket) {
    if (queue->head == NULL) return 0;
    struct socketQueueElem* tmp;
    for (tmp = queue->head; tmp != NULL; tmp = tmp->next) {
        if (tmp->socket == socket) return 1;
    }
    return 1;
}

int enqueue(struct requestSocketQueue* queue, int socket, unsigned char* firstByte) {
    if (queue->head == NULL) {
        queue->head = malloc(sizeof(struct socketQueueElem*)*1000000);
        queue->head->socket = socket;
        queue->head->next = NULL;
        queue->head->firstByte = malloc(1);
        memcpy(queue->head->firstByte,firstByte,1);
        return 0;  
    }
    struct socketQueueElem* tmp = queue->head;
    struct socketQueueElem* toAdd = malloc(sizeof(struct socketQueueElem));
    toAdd->socket = socket;
    toAdd->firstByte = malloc(1);
    memcpy(firstByte,toAdd->firstByte,1);    
    toAdd->next = NULL;
    while(1) {
        if (tmp->next == NULL) break;
        tmp = tmp->next;
    }
    tmp->next = NULL;
    return 0;
}

struct socketQueueElem* dequeue(struct requestSocketQueue* queue, int socket) {
    struct socketQueueElem* tmp = queue->head;
    queue->head = tmp->next;
    return tmp;
}


