struct requestSocketQueue {
    struct socketQueueElem* head;
    int size;
};

struct socketQueueElem {
    int socket;
    unsigned char* firstByte;
    struct socketQueueElem* next;
};

struct requestSocketQueue;

struct socketQueueElem;

int is_in_the_queue(struct requestSocketQueue* queue, int socket);

int enqueue(struct requestSocketQueue* queue, int socket, unsigned char* fisrtByte);

struct socketQueueElem* dequeue(struct requestSocketQueue* queue, int socket);