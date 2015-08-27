#include "CPUActivation.hpp"
#include "ThreadManager.hpp"
#include "WorkerThread.hpp"
#include "scheduling/Scheduler.hpp"
#include "tasks/Task.hpp"


#include <pthread.h>


__thread WorkerThread *WorkerThread::_currentWorkerThread = nullptr;


WorkerThread::WorkerThread(CPU *cpu)
	: _suspensionConditionVariable(), _cpu(cpu), _task(nullptr)
{
	int rc = pthread_create(&_pthread, &_cpu->_pthreadAttr, (void* (*)(void*)) &WorkerThread::body, this);
	assert(rc == 0);
}


void *WorkerThread::body()
{
	ThreadManager::threadStartup(this);
	
	while (!ThreadManager::mustExit()) {
		CPUActivation::activationCheck(this);
		
		_task = Scheduler::schedule(_cpu);
		
		if (_task != nullptr) {
			handleTask();
		} else {
			ThreadManager::yieldIdler(this);
		}
	}
	
	ThreadManager::exitAndWakeUpNext(this);
	
	assert(false);
	return nullptr;
}


void WorkerThread::handleTask()
{
	// Run the task
	_task->setThread(this);
	_task->body();
	
	// TODO: Release successors
	
	// Follow up the chain of ancestors and dispose them as needed and wake up any in a taskwait that finishes in this moment
	{
		bool readyOrDisposable = _task->markAsFinished();
		Task *currentTask = _task;
		
		while ((currentTask != nullptr) && readyOrDisposable) {
			Task *parent = currentTask->getParent();
			
			if (currentTask->hasFinished()) {
				readyOrDisposable = currentTask->unlinkFromParent();
				delete currentTask; // FIXME: Need a proper object recycling mechanism here
				currentTask = parent;
			} else {
				// An ancestor in a taskwait that finishes at this point
				ThreadManager::threadBecomesReady(currentTask->getThread());
				readyOrDisposable = false;
			}
		}
	}
	
	_task = nullptr;
}

