import logging



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



        #if the lock table does not contain the key entry yet, you should create one.
        if key not in self._lock_table.keys():
            #Lock Table contains current lock mode and granted group
            #Lock Table contains waiting a queue of transactions and lock mode
            self._lock_table[key] = [ [['e', self]] , [] ]
            self._acquired_locks.append( [key, ['e', self]] )

            preV = self._store.get(key)
            self._store.put(key, value)
            self._undo_log.append((key, preV))   
            return 'Success'
        else:
            grantedGroup = self._lock_table[key][0]
            waitingGroup = self._lock_table[key][1]
            #If grantedGroup is empty
            if len(grantedGroup)== 0:
                self._lock_table[key][0].append(['e', self])
                self._acquired_locks.append( [key, ['e', self]])
                #For log
                v = self._store.get(key)
                self._undo_log.append( [key,v] )
                self._store.put(key, value)
                return 'Success'
            #If we have grantedGroup
            else:
                mode = self._lock_table[key][0][0][0]
                #if it is a shared Group
                if mode == 's':
                    #if current transaction is in grantedGroup
                    #And, there is no other transcation waiting for this lock
                    if ['s', self] in grantedGroup and len(waitingGroup)==0:
                        #check if it is the only transaction holding shared lock
                        #If it is, we can upgrade to exclusive lock, then, PUT
                        if len(grantedGroup)==1:
                            #update mode to exclusive
                            self._lock_table[key][0][0][0] = 'e'
                            #update acquired_locks
                            acIndex = self._acquired_locks.index([key, ['s', self]])
                            self._acquired_locks[acIndex][1][0] = 'e'
                            #For log
                            preV = preV = self._store.get(key)
                            self._undo_log.append((key, preV))
                            self._store.put(key, value)
                            return 'Success'
                        #If there are other transactions sharing this lock
                        #we put this transaction in waitingGroup and desired_lock
                        #since other transactions are using it
                        elif len(grantedGroup)>1:
                            self._lock_table[key][1].append(['e',self])
                            self._desired_lock = [key, ['e',self], value]
                            
                            return None
                    #If this transaction is not in the grantedGroup, 
                    #And, the lock is shared by other transaction/transactions
                    #we put transaction in waitingGroup, and desired_locks
                    else: 
                        self._lock_table[key][1].append(['e',self])
                        self._desired_lock = [key, ['e',self], value]
                        
                        return None
                elif mode == 'e':
                    #if this transaction is currently holding exclusive lock,
                    #Success
                    if ['e', self] in grantedGroup:
                        #For log
                        preV = self._store.get(key)
                        self._undo_log.append((key, preV))
                        self._store.put(key, value)
                        return 'Success'
                    #If this exclusive lock is held by other transaction, 
                    #We put this in our waitingGroup, and desired_lock
                    else:
                        self._lock_table[key][1].append(['e',self])
                        self._desired_lock = [key, ['e',self], value]
                        return None

        

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
        if key not in self._lock_table.keys():
            #Lock Table contains current lock mode and granted group
            #Lock Table contains waiting a queue of transactions and lock mode
            self._lock_table[key] = [ [['s', self]] , [] ]
            self._acquired_locks.append([key, ['s', self]])
            value = self._store.get(key)
            if value is None:
                return 'No such key'
            else:
                return value
        #if there is key
        else:
            grantedGroup = self._lock_table[key][0]
            waitingGroup = self._lock_table[key][1]
            #If there is no grantedGroup and no waitingGroup
            if len(grantedGroup) == 0 and len(waitingGroup) == 0:
                #set lock for this transaction
                self._lock_table[key][0].append( ['s', self] )
                self._acquired_locks.append( [key, ['s', self]] )
                value = self._store.get(key)
                if value is None:
                    return 'No such key'
                return value
            #If there is grantedGroup
            else:
                mode = self._lock_table[key][0][0][0]
                #if the grantedGroup is for sharing    
                if mode == 's':
                    #If this transaction is already in grantedGroup
                    if ['s', self] in grantedGroup:
                        value = self._store.get(key)
                        if value is None:
                            return 'No such key'
                        return value
                    #If this is not in grantedGroup,
                    else:
                        #If there is no waiting group,
                        #It is compatible
                        if len(waitingGroup) == 0:
                            self._lock_table[key][0].append( ['s', self] )
                            self._acquired_locks.append( [key, ['s', self]] )
                            value = self._store.get(key)
                            if value is None:
                                return 'No such key'
                            return value
                        #If there is a waiting group,
                        #It is not compatible
                        else:
                            self._lock_table[key][1].append( ['s',self] )
                            self._desired_lock = [key, ['s', self], 0]
                            return None
                elif mode == 'e':
                    #If this transection has exclusive lock on this key,
                    if ['e', self] in grantedGroup:
                        value = self._store.get(key)
                        if value is None:
                            return 'No such key'
                        return value
                    #if other transaction is holding a e-lock on this key
                    #we put it in queue, _desired_locks
                    else:
                        self._lock_table[key][1].append( ['s',self] )
                        self._desired_lock = [key, ['s', self], 0]
                        return None

        




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
        
        # release the transaction from grantedGroup
        if self._desired_lock is not None:
           waitingKey = self._desired_lock[0]
           waitingTransaction = self._desired_lock[1]
           idx = self._lock_table[waitingKey][1].index(waitingTransaction)
           #release the transaction from grantGroup
           del self._lock_table[waitingKey][0][idx]


        #Releases all locks acquired by the transaction and grants them to the
        #next transactions in the queue.  _acquired_locks:[ [key, [self, 's']], [key, [self, 's']], ...]
        for l in self._acquired_locks:
            key = l[0] 
            trans = l[1] 
            
            #if there is other transaction sharing the current lock, 
            if len(self._lock_table[key][0])>1:
                #release the lock, and the transaction from grantedGroup
                idx = self._lock_table[key][0].index(trans)
                del self._lock_table[key][0][idx]
                #Check if the one left is able to be upgraded
                if len(self._lock_table[key][0]) == 1: #[ [['s', self]] , [] ]
                    transaction = self._lock_table[key][0][0]
                    mode = transaction[0]
                    handler = transaction[1]
                    #if transaction is shared and it is also waiting for exclusive lock,
                    if mode == 's' and ['e' , handler] in  self._lock_table[key][1]:
                        #delete it from waiting group
                        idx = self._lock_table[key][1].index(['e', handler])
                        del self._lock_table[key][1][idx]
                        #update grantedGroup in self lock_table
                        self._lock_table[key][0] = [ ['e', handler] ]
                        #update _acquired_locks
                        acIndex = handler._acquired_locks.index([key, ['s', handler]])
                        handler._acquired_locks[acIndex][1][0] = 'e'
           
            #if other transation are not sharing this lock
            else:
                waitingGroup = self._lock_table[key][1]
                #if there is no waiting group
                if len(waitingGroup)== 0:
                    self._lock_table[key] = [ [],[] ]
                #if there is waiting group
                else:
                    #if the first dequeued one is waiting for exclusive lock, 
                    #we grant exclusive lock to this one, and update the acquiring handler 
                    if waitingGroup[0][0] == 'e':
                        waitingTransaction  = self._lock_table[key][1].pop()
                        waitingHandler = waitingTransaction[1]
                        self._lock_table[key][0] = [waitingTransaction]
                        waitingHandler._acquired_locks.append([key, waitingTransaction])
                    #if the first dequeued one is waiting for shared lock, 
                    #find new grantedGroup
                    else:
                        numReaders = 0;
                        self._lock_table[key][0] = []
                        for t in waitingGroup:
                            if t[0] == 'e':
                                break
                            #add to grantedGroup
                            self._lock_table[key][0].append(t)
                            #update 
                            t[1]._acquired_locks.append( [key, t])
                            numReaders+=1
                        for i in range(numReaders):
                            self._lock_table[key][1].pop()



        self._undo_log = []                    
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

        key = self._desired_lock[0]
        transaction = self._desired_lock[1]
        mode = transaction[0]
        value = self._desired_lock[2]
        

        if[key, transaction] in self._acquired_locks:
            self._desired_lock = None
            if mode == 'e':
                return self.perform_put(key, value)
            else:
                return self.perform_get(key)
        else:
            return None
        






"""
Part II: Implement deadlock detection method for the transaction coordinator

The transaction coordinator has access to the following object:

self._lock_table: see description from Part I
"""

class TransactionCoordinator:

    def __init__(self, lock_table):
        self._lock_table = lock_table

    def dfs(self, waitsForGraph, w_xid, visited=None):

        if w_xid not in waitsForGraph:
            return True
        
        if visited is None:
            visited = set()

        if w_xid in visited:
            return False

        visited.add(w_xid)

        for g in waitsForGraph[w_xid]:
            if self.dfs(waitsForGraph, g, visited) == False:
                return False
        return True

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
        
        #Construct waitsForGraph using Dictionary
        waitsForGraph = {}                                  
        for key in self._lock_table.keys():                 
            for w in self._lock_table[key][1]:         
                for g in self._lock_table[key][0]:
                    w_xid = w[1]._xid
                    g_xid = g[1]._xid
                    if w_xid not in waitsForGraph.keys():
                        waitsForGraph[w_xid] = [g_xid]
                    else:
                        waitsForGraph[w_xid].append(g_xid)

        #Detect cycle
        for w_xid in waitsForGraph.keys():
            if self.dfs(waitsForGraph, w_xid) == False:
                return w_xid
        return None

