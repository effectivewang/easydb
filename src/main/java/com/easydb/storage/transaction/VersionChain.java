package com.easydb.storage.transaction;

import java.util.*;
import java.util.concurrent.locks.*;

// Version chain management
public class VersionChain {
    private volatile Version head;
    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    public void addVersion(Version version) {
        lock.writeLock().lock();
        try {
            version.setNext(head);
            head = version;
        } finally {
            lock.writeLock().unlock();
        }
    }

    public Version getLatestVersion() {
        lock.readLock().lock();
        try {
            return head;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Version getLatestCommittedVersion() {
        lock.readLock().lock();
        try {
            return head;
        } finally {
            lock.readLock().unlock();
        }
    }

    public Version getVersionAtTimestamp(long timestamp) {
        lock.readLock().lock();
        try {
            Version current = head;
            while (current != null) {
                if (current.getCommitTs() <= timestamp) {
                    return current;
                }
                current = current.getNext();
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    public List<Version> getAllVersions() {
        lock.readLock().lock();
        try {
            List<Version> versions = new ArrayList<>();
            Version current = head;
            while (current != null) {
                versions.add(current);
                current = current.getNext();
            }
            return versions;
        } finally {
            lock.readLock().unlock();
        }
    }

}