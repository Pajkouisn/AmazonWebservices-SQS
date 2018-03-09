# SQS Library

## This library provides a simple interface for interaction with AWS SQS.

### Functions included:

* Getting A new Amazon SQS client

```
	* getSQSClient() - Get a new SQS Client
    * getSQSClient(String swfAccessId, String swfSecretKey) - Using credentials.
```

* Getters

```
	* getQueueUrl() - Get the queue URL.
	* isQueueEmpty() - Returns true if queue is empty.
	* doesQueueExist() - Returns true if queue does exist.
```

* Purging the queue
```
	* purgeQueue() and purgeQueue(String) can be used to purge queues.
```

* Send Message to queue functions
```
	* sendMessage(String message) - Send message to the default URL.
	* sendMessage(String message, int n) - Send message to the default URL n times.
	* sendMessage(String message, String url) - Send message to a customised URL.
	* sendMessage(String message, String url, int n) - Send message to a customised URL. 
	* sendMessageWithMessageId(String message, String messageGroupId) - Send message to the default URL with message ID (Required for FIFO queues).
    
```

* Get Message functions
```
	* getMessage() - Get a message from the queue.
	* getMessages(int n) - Get a list of n messages from the queue.
	* getAllUniqueMessages() - Get a list of unique messages from the queue.
	* getAllMessages() - Get all messages from the queue.
	* getAndLockMessage() - Get and lock a single message from teh queue.
	* getAndLockMessages(int n) - Get and lock n message from the queue.
	* getAndLockAllMessages() - Get and lock all messages from the queue 
```

* Dequeue
```
	* dequeueMessage() - Dequeue a message from the queue.
	* dequeueAllMessages() - Dequeue and Get all messages from the queue.
```

* Lock and Unlock Messages
```
	* lockMessages(String sessionId, int seconds) - Lock a message. 
	* lockMessages(List<String> sessionIds, int seconds) - Lock a list of messages.
	* unlockMessages(String sessionId) - Unlock a single message.
	* unlockMessages(List<String> sessionIds) - Unlock a list of messages.
```

* Delete messages
```
	* deleteMessage(String sessionId) - Delete message with the following session ID.
	* deleteMessages(List<String> sessionIds) - Delete messages with the following session IDS.
```

* Transfer functions
```
	* moveMessages(boolean useMessageId, boolean useQueueName, String...strings) - Move all messages from 1 queue to another.
```

* Queue info
```
	* getCount() - Get the number of messages in queue.
	* getQueueAttributes () - Get all queue attributes.
```

* Finding queue URL
```
	* findQueueUrl(String queueName) - Returns the queue URL.
	* findAndSetQueueUrl(String queueName) - Returns and sets the Queue URL.
```

* Listing Queues
```
	* listQueues() - This function can be used to list 	queues.
```

* Create and delete
```
	* createQueue(String queueName) - This function creates a new queue.
	* deleteQueue() - This function deletes an existing queue (Must be set in object url).
```

