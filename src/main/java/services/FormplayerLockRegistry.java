package services;

import exceptions.InterruptedRuntimeException;
import io.sentry.event.Event;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.joda.time.DateTime;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.integration.support.locks.LockRegistry;
import org.springframework.util.Assert;
import util.Constants;
import util.FormplayerSentry;

import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by willpride on 11/6/17.
 */
public class FormplayerLockRegistry implements LockRegistry {

    @Autowired
    private FormplayerSentry raven;

    private final FormplayerReentrantLock[] lockTable;
    // Lock objects for accessing the FormplayerReentrantLock table above
    // These objects do not change
    private final Object[] lockTableLocks;

    private final int mask;

    private final Log log = LogFactory.getLog(FormplayerLockRegistry.class);

    public FormplayerLockRegistry() {
        this(0xFF);
    }

    public FormplayerLockRegistry(int mask) {
        String bits = Integer.toBinaryString(mask);
        Assert.isTrue(bits.length() < 32 && (mask == 0 || bits.lastIndexOf('0') < bits.indexOf('1')), "Mask must be a power of 2 - 1");
        this.mask = mask;
        int arraySize = this.mask + 1;
        this.lockTable = new FormplayerReentrantLock[arraySize];
        this.lockTableLocks = new Object[arraySize];
        for (int i = 0; i < arraySize; i++) {
            this.lockTable[i] = new FormplayerReentrantLock();
            this.lockTableLocks[i] = new Object();
        }
    }

    private FormplayerReentrantLock setNewLock(Integer lockIndex) {
        synchronized (this.lockTableLocks[lockIndex]) {
            this.lockTable[lockIndex] = new FormplayerReentrantLock();
            return this.lockTable[lockIndex];
        }
    }

    @Override
    public FormplayerReentrantLock obtain(Object lockKey) {
        Assert.notNull(lockKey, "'lockKey' must not be null");
        Integer lockIndex = lockKey.hashCode() & this.mask;

        synchronized (this.lockTableLocks[lockIndex]) {
            FormplayerReentrantLock lock = this.lockTable[lockIndex];

            Thread ownerThread = lock.getOwner();

            if (!ownerThread.isAlive()) {
                log.error(String.format("Evicted dead thread %s owning lockkey %s.", ownerThread, lockKey));
                lock = setNewLock(lockIndex);
            }
            if (lock.isExpired()) {
                log.error(String.format("Thread %s owns expired lock with lock key %s.", ownerThread, lockKey));
                ownerThread.interrupt();
                try {
                    ownerThread.join(5000);
                } catch (InterruptedException e) {
                    throw new InterruptedRuntimeException(e);
                }
                if (ownerThread.isAlive()) {
                    log.error(String.format("Unable to evict thread %s owning expired lock with lock key %s.", ownerThread, lockKey));
                    Exception e = new Exception("Unable to get expired lock, owner thread has stack trace");
                    e.setStackTrace(ownerThread.getStackTrace());
                    raven.sendRavenException(new Exception(e), Event.Level.WARNING);
                }
            }
            return lock;
        }
    }

    public class FormplayerReentrantLock extends ReentrantLock {

        DateTime lockTime;

        @Override
        public boolean tryLock(long timeout, TimeUnit unit)
                throws InterruptedException {
            boolean obtainedLock = super.tryLock(timeout, unit);
            if (obtainedLock) {
                lockTime = new DateTime();
            }
            return obtainedLock;
        }

        public Thread getOwner() {
            return super.getOwner();
        }

        public boolean isExpired() {
            return new DateTime().minusMillis(Constants.LOCK_DURATION).isAfter(lockTime);
        }
    }
}
