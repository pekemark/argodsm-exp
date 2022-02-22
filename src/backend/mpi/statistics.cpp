#include <stdio.h>

#include "statistics.hpp"

argo_statistics stats;

void argo_statistics::clear() {
    this->locktime = 0;
    this->selfinvtime = 0;
    this->loadtime = 0;
    this->storetime = 0;
    this->writebacktime = 0;
    this->flushtime = 0;
    this->barriertime = 0;
    this->stores = 0;
    this->loads = 0;
    this->barriers = 0;
    this->writebacks = 0;
    this->locks = 0;
    this->unlocks = 0;
    this->locktransfers = 0;
    this->ssitime = 0;
    this->ssdtime = 0;
}

void argo_statistics::print(int node){
    printf("[N%d] #####################STATISTICS#########################\n", node);
    printf("[N%d] # PROCESS ID %d \n", node, node);
    printf("[N%d] # locktime: %lf \n", node, locktime);
    printf("[N%d] # selfinvtime: %lf \n", node, selfinvtime);
    printf("[N%d] # loadtime: %lf \n", node, loadtime);
    printf("[N%d] # storetime: %lf \n", node, storetime);
    printf("[N%d] # writebacktime: %lf \n", node, writebacktime);
    printf("[N%d] # flushtime: %lf \n", node, flushtime);
    printf("[N%d] # barriertime: %lf \n", node, barriertime);
    printf("[N%d] # stores: %lu \n", node, stores);
    printf("[N%d] # loads: %lu \n", node, loads);
    printf("[N%d] # barriers: %lu \n", node, barriers);
    printf("[N%d] # writebacks: %lu \n", node, writebacks);
    printf("[N%d] # locks: %lu \n", node, locks);
    printf("[N%d] # unlocks: %lu \n", node, unlocks);
    printf("[N%d] # locktransfers: %lu \n", node, locktransfers);
    printf("[N%d] # ssitime: %lf \n", node, ssitime);
    printf("[N%d] # ssdtime: %lf \n", node, ssdtime);
    printf("[N%d] ########################################################\n", node);
    printf("\n\n");
}