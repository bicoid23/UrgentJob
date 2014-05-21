/**
 * File: thread-pool.cc
 * --------------------
 * Presents the implementation of the ThreadPool class.
 */

#include "thread-pool.h"
#include <functional>
#include "semaphore.h"
#include <queue>
#include <stdbool.h>
#include <iostream>
using namespace std;

queue<function<void(void)>> jobsQueue;
mutex jobsQueueLock;
condition_variable_any jobsQueueCv;

semaphore numberOfUnclaimedJobs(0);

semaphore numberOfWorkersAvailable(0);
semaphore dispatcherAvailable(0);

int numJobsRunning;
mutex numJobsRunningLock;
condition_variable_any numJobsRunningCv;

bool isThPoolDone;

int numTotalThreads;

void dispatcher() {
  while(true) {
    numberOfUnclaimedJobs.wait();
    if (isThPoolDone) break;

    numberOfWorkersAvailable.wait(); // wait for a worker to become available
    dispatcherAvailable.signal();// signal the worker thread to execute it
  }
}

void worker(size_t workerID) {
  while (true) {
    numberOfWorkersAvailable.signal(); //
    dispatcherAvailable.wait(); // wait for dispatcher thread to signal to execute a function
    
    if (isThPoolDone) break;

    numJobsRunningLock.lock();
    numJobsRunning++;
    numJobsRunningLock.unlock();
    
    jobsQueueLock.lock();
    const function<void(void)>& thunk = jobsQueue.front();
    jobsQueue.pop();
    if (jobsQueue.size() == 0) jobsQueueCv.notify_all();
    jobsQueueLock.unlock();

    thunk();

    numJobsRunningLock.lock();
    numJobsRunning--;
    if (numJobsRunning == 0) numJobsRunningCv.notify_all();
    numJobsRunningLock.unlock();
  }
}

ThreadPool::ThreadPool(size_t numThreads) : wts(numThreads) {
  numJobsRunning = 0;
  numTotalThreads = numThreads;
  isThPoolDone = false;

  dt = thread([this]() {
    dispatcher();
  });

  for (size_t workerID = 0; workerID < numThreads; workerID++) {
    wts[workerID] = thread([this](size_t workerID) {
      worker(workerID);
    }, workerID);
  }
}

void ThreadPool::schedule(const function<void(void)>& thunk) {
  jobsQueueLock.lock();
  jobsQueue.push(thunk);
  jobsQueueLock.unlock();
  numberOfUnclaimedJobs.signal();
  return;
}

void ThreadPool::wait() {
  // block until all scheduled functions have been executed
  lock_guard<mutex> lg1(numJobsRunningLock);
  lock_guard<mutex> lg2(jobsQueueLock);

  jobsQueueCv.wait(jobsQueueLock, []{
    return jobsQueue.size() == 0;
  });

  numJobsRunningCv.wait(numJobsRunningLock, []{
    return numJobsRunning == 0;
  });
}

ThreadPool::~ThreadPool() {
  wait();
  isThPoolDone = true;
  numberOfUnclaimedJobs.signal();
  for (int i = 0; i < numTotalThreads; i++)
    dispatcherAvailable.signal();
}
