import logging
from Queue import PriorityQueue


from kvstore import DBMStore, InMemoryKVStore

LOG_LEVEL = logging.WARNING

KVSTORE_CLASS = InMemoryKVStore

"""
Possible abort modes.
"""
USER = 0
DEADLOCK = 1

"""
Part I: Implementing request handling methods for the transaction handler

The transaction handler has access to the following objects:

self._lock_table: the global lock table. More information in the README.

self._acquired_locks: a list of locks acquired by the transaction. Used to
release locks when the transaction commits or aborts. This list is initially
empty.

self._desired_lock: the lock that the transaction is waiting to acquire as well
as the operation to perform. This is initialized to None.

self._xid: this transaction's ID. You may assume each transaction is assigned a
unique transaction ID.

self._store: the in-memory key-value store. You may refer to kvstore.py for
methods supported by the store.

self._undo_log: a list of undo operations to be performed when the transaction
is aborted. The undo operation is a tuple of the form (@key, @value). This list
is initially empty.

You may assume that the key/value inputs to these methods are already type-
checked and are valid.
"""

class TransactionHandler:

    def __init__(self, lock_table, xid, store):
        self._lock_table = lock_table
        self._acquired_locks = []
        self._desired_lock = None
        self._xid = xid
        self._store = store
        self._undo_log = []

    def perform_put(self, key, value):
        """
        Handles the PUT request. You should first implement the logic for
        acquiring the exclusive lock. If the transaction can successfully
        acquire the lock associated with the key, insert the key-value pair
        into the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.
        Hint: be aware that lock upgrade may happen.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted. See the code in abort()
        for the exact format.

        @param self: the transaction handler.
        @param key, value: the key-value pair to be inserted into the store.

        @return: if the transaction successfully acquires the lock and performs
        the insertion/update, returns 'Success'. If the transaction cannot
        acquire the lock, returns None, and saves the lock that the transaction
        is waiting to acquire in self._desired_lock.
        """
        # Part 1.1: your code here!
        """
        As discussed in class, the lock table needs to keep track, for each key in 
        the table, of two basic things:

        Which transactions currently have a lock on that key (the "granted group" of transactions), 
        and what the lock mode is.
        A queue of transactions that are waiting to be granted a lock on that key -- 
        and the lock mode that they are waiting for.

        """



        #if there is no lock_table, we create one
        if len(self._lock_table.keys())== 0:
            self._lock_table[key] = ['e', [self]]
            self._acquired_locks.append([key, [self, 'e']])
            self._store.put(key, value)
            self._undo_log.append((key, None))
            
            return 'Success'
        else:
            if key in self._lock_table:
                mode = self._lock_table[key][0]
                transactionList = self._lock_table[key][1]
                if mode == 's':
                    #if current transaction is currently sharing this lock
                    if self in transactionList:
                        #check if it is the only transaction holding shared lock
                        #If it is, we can upgrade to exclusive lock, then, PUT
                        if len(transactionList)==1 :
                            self._lock_table[key][0] = 'e'
                            self._acquired_locks.append([key, [self, 'e']])
                            self._store.put(key, value)
                            self._undo_log.append((key, None))
                            return 'Success'
                        #If there are other transactions sharing this lock
                        #we put this transaction in queue 
                        #since other transactions are using it
                        else:
                            if self._desired_lock:
                                self._desired_lock.put((key, self, 'e'))
                                #desired_lock.put(key, self.xid, 'e')
                                return None
                            else:
                                self._desired_lock = PriorityQueue()
                                self._desired_lock.put((key, self, 'e'))
                                #desired_lock.put(key, self.xid, 'e')
                                return None
                    #If this transaction is not in the list, 
                    #And, the lock is shared by other transaction/transactions
                    #we put this in queue
                    else: 
                        if self._desired_lock:
                            self._desired_lock.put((key, self, 'e'))
                            #desired_lock.put(key, self.xid, 'e')
                            return None
                        else:
                            self._desired_lock = PriorityQueue()
                            self._desired_lock.put((key, self, 'e'))
                            #desired_lock.put(key, self.xid, 'e')
                            return None
                elif mode == 'e':
                    #if this transaction is currently holding exclusive lock,
                    #Success
                    if transactionList[0]==self:
                        self._lock_table[key][0] = 'e'
                        self._acquired_locks.append([key, [self, 'e']])
                        self._store.put(key, value)
                        self._undo_log.append((key, None))
                        return 'Success'
                    else:
                        if self._desired_lock:
                            self._desired_lock.put((key, self, 'e'))
                            #desired_lock.put(key, self.xid, 'e')
                            return None
                        else:
                            self._desired_lock = PriorityQueue()
                            self._desired_lock.put((key, self, 'e'))
                            #desired_lock.put(key, self.xid, 'e')
                            return None


        #else we need to put the lock in self._desired_lock and return None
            #if key is not in our lock_table, we can grant the lock to this transac
            else: 
                self._lock_table[key] = ['e',[self]]
                self._acquired_locks.append([key, [self, 'e']])
                self._store.put(key, value)
                self._undo_log.append((key, None))
                return 'Success'

        

    def perform_get(self, key):
        """
        Handles the GET request. You should first implement the logic for
        acquiring the shared lock. If the transaction can successfully acquire
        the lock associated with the key, read the value from the store.

        Hint: if the lock table does not contain the key entry yet, you should
        create one.

        @param self: the transaction handler.
        @param key: the key to look up from the store.

        @return: if the transaction successfully acquires the lock and reads
        the value, returns the value. If the key does not exist, returns 'No
        such key'. If the transaction cannot acquire the lock, returns None,
        and saves the lock that the transaction is waiting to acquire in
        self._desired_lock.
        """
        # Part 1.1: your code here!
        #if there is no lock_table, we create one
        if len(self._lock_table.keys())== 0:
            self._lock_table[key] = ['s', [self]]
            self._acquired_locks.append([key, [self, 's']])
            #self._undo_log.append(key, None)
            #self._desired_lock.pop()
            value = self._store.get(key)
            if value is None:
                return 'No such key'
            else:
                return value
        #if there is lock_table
        else:
            #If key is in our lock_table, and if it is shared lock,
            #we need this transaction to the list for share
            if key in self._lock_table:
                mode = self._lock_table[key][0]
                transactionList = self._lock_table[key][1]
                if mode == 's':
                    if self not in transactionList:
                        self._lock_table[key][1].append(self)
                        self._acquired_locks.append([key, [self, 's']])
                        #self._undo_log.append(key, None)
                        #self._desired_lock.pop()
                        value = self._store.get(key)
                        if value is None:
                            return 'No such key'
                        else:
                            return value
                    #If this transaction is already sharing this lock
                    else:
                        value = self._store.get(key)
                        if value is None:
                            return 'No such key'
                        else:
                            return value
                #If it is exclusive lock,
                elif mode == 'e':
                    #if other transaction is holding a e-lock on this key
                    #we put it in queue
                    if self != transactionList[0]:
                        if self._desired_lock:
                            self._desired_lock.put((key, self, 'e'))
                            #desired_lock.put(key, self.xid, 'e')
                            return None
                        else:
                            self._desired_lock = PriorityQueue()
                            self._desired_lock.put((key, self, 'e'))
                            #desired_lock.put(key, self.xid, 'e')
                            return None
                    #If this lock already has exclusive lock, we return value
                    else:
                        value = self._store.get(key)
                        if value is None:
                            return 'No such key'
                        else:
                            return value

    
            #If lock_table does not have this key
            #No other transaction is locking this key
            else:
                self._lock_table[key] = ['s', [self]]
                self._acquired_locks.append([key, [self, 's']])
                value = self._store.get(key)
                if value is None:
                    return 'No such key'
                else:
                    return value

    def release_and_grant_locks(self):
        """
        Releases all locks acquired by the transaction and grants them to the
        next transactions in the queue. This is a helper method that is called
        during transaction commits or aborts. 

        Hint: you can use self._acquired_locks to get a list of locks acquired
        by the transaction.
        Hint: be aware that lock upgrade may happen.

        @param self: the transaction handler.
        """
        """
        1) The first xact that wants an X lock - and no other xacts
        2) The first n consecutive xacts that want S locks. This set will end at the 
    first xact that wants a X lock - or at the end of the queue
        """
        # clear waiting queue
        #if self._desired_lock is not None:


        #Releases all locks acquired by the transaction and grants them to the
        #next transactions in the queue.  _acquired_locks:[ [key, [self, 's']], [key, [self, 's']], ...]
        for l in self._acquired_locks:
            pass
            #key = l[0]
            #axt = l[1]
            #mode = _desired_lock.peek()[2]

            
           # self._lock_table.clear()
        self._acquired_locks = []

    def commit(self):
        """
        Commits the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.

        @return: returns 'Transaction Completed'
        """
        self.release_and_grant_locks()
        return 'Transaction Completed'

    def abort(self, mode):
        """
        Aborts the transaction.

        Note: This method is already implemented for you, and you only need to
        implement the subroutine release_locks().

        @param self: the transaction handler.
        @param mode: mode can either be USER or DEADLOCK. If mode == USER, then
        it means that the abort is issued by the transaction itself (user
        abort). If mode == DEADLOCK, then it means that the transaction is
        aborted by the coordinator due to deadlock (deadlock abort).

        @return: if mode == USER, returns 'User Abort'. If mode == DEADLOCK,
        returns 'Deadlock Abort'.
        """
        while (len(self._undo_log) > 0):
            k,v = self._undo_log.pop()
            self._store.put(k, v)
        self.release_and_grant_locks()
        if (mode == USER):
            return 'User Abort'
        else:
            return 'Deadlock Abort'

    def check_lock(self):
        """
        If perform_get() or perform_put() returns None, then the transaction is
        waiting to acquire a lock. This method is called periodically to check
        if the lock has been granted due to commit or abort of other
        transactions. If so, then this method returns the string that would 
        have been returned by perform_get() or perform_put() if the method had
        not been blocked. Otherwise, this method returns None.

        As an example, suppose Joe is trying to perform 'GET a'. If Nisha has an
        exclusive lock on key 'a', then Joe's transaction is blocked, and
        perform_get() returns None. Joe's server handler starts calling
        check_lock(), which keeps returning None. While this is happening, Joe
        waits patiently for the server to return a response. Eventually, Nisha
        decides to commit his transaction, releasing his exclusive lock on 'a'.
        Now, when Joe's server handler calls check_lock(), the transaction
        checks to make sure that the lock has been acquired and returns the
        value of 'a'. The server handler then sends the value back to Joe.

        Hint: self._desired_lock contains the lock that the transaction is
        waiting to acquire.
        Hint: remember to update the self._acquired_locks list if the lock has
        been granted.
        Hint: if the transaction has been granted an exclusive lock due to lock
        upgrade, remember to clean up the self._acquired_locks list.
        Hint: remember to update self._undo_log so that we can undo all the
        changes if the transaction later gets aborted.

        @param self: the transaction handler.

        @return: if the lock has been granted, then returns whatever would be
        returned by perform_get() and perform_put() when the transaction
        successfully acquired the lock. If the lock has not been granted,
        returns None.
        """
        pass # Part 1.3: your code here!







"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I
"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table

    def detect_deadlocks(self):
        """
        Constructs a waits-for graph from the lock table, and runs a cycle
        detection algorithm to determine if a transaction needs to be aborted.
        You may choose which one transaction you plan to abort, as long as your
        choice is deterministic. For example, if transactions 1 and 2 form a
        cycle, you cannot return transaction 1 sometimes and transaction 2 the
        other times.

        This method is called periodically to check if any operations of any
        two transactions conflict. If this is true, the transactions are in
        deadlock - neither can proceed. If there are multiple cycles of
        deadlocked transactions, then this method will be called multiple
        times, with each call breaking one of the cycles, until it returns None
        to indicate that there are no more cycles. Afterward, the surviving
        transactions will continue to run as normal.

        Note: in this method, you only need to find and return the xid of a
        transaction that needs to be aborted. You do not have to perform the
        actual abort.

        @param self: the transaction coordinator.

        @return: If there are no cycles in the waits-for graph, returns None.
        Otherwise, returns the xid of a transaction in a cycle.
        """
        pass # Part 2.1: your code here!
