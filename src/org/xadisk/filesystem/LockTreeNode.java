/*
Copyright © 2010-2011, Nitin Verma (project owner for XADisk https://xadisk.dev.java.net/). All rights reserved.

This source code is being made available to the public under the terms specified in the license
"Eclipse Public License 1.0" located at http://www.opensource.org/licenses/eclipse-1.0.php.
*/

package org.xadisk.filesystem;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicReference;

public class LockTreeNode {

    private final File path;
    private NativeLock lock;
    private final ConcurrentHashMap<String, LockTreeNode> children = new ConcurrentHashMap<String, LockTreeNode>();
    private final AtomicReference<TransactionInformation> pinHolder = new AtomicReference<TransactionInformation>(null);

    LockTreeNode(File path, boolean withExclusiveLock) {
        this.path = path;
        this.lock = new NativeLock(withExclusiveLock, path);
    }

    LockTreeNode getChild(String name) {
        LockTreeNode node = children.get(name);
        if (node != null) {
            return node;
        } else {
            node = new LockTreeNode(new File(path, name), false);
            LockTreeNode olderValue = children.putIfAbsent(name, node);
            if(olderValue == null) {
                return node;
            } else {
                return olderValue;
            }
        }
    }

    LockTreeNode[] getAllChildren() {
        return children.values().toArray(new LockTreeNode[0]);
    }
    
    boolean isPinnedByOtherTransaction(TransactionInformation thisTransaction) {
        return !(pinHolder.get() == null || pinHolder.get().equals(thisTransaction));
    }

    boolean attemptPinning(TransactionInformation requestor) {
        TransactionInformation holderTransaction = pinHolder.get();
        if(holderTransaction == null) {
            return pinHolder.compareAndSet(null, requestor);
        } else {
            return holderTransaction.equals(requestor);
        }
    }

    void releasePin() {
        pinHolder.set(null);
    }

    NativeLock getLock() {
        return lock;
    }

    File getPath() {
        return path;
    }
}
