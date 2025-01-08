package com.easydb.storage.transaction;

import com.easydb.core.Tuple;
import com.easydb.core.TupleId;
import java.util.*;

public class Version {
    private final Tuple data;
    private final long beginTs;  // Transaction start timestamp
    private long commitTs; // Transaction commit timestamp
    private Version next;  // Previous version in chain
    private final long xid;      // Transaction ID that created this version
    
    public Version(Tuple data, long beginTs, long commitTs, long xid, Version next) {
        this.data = data;
        this.beginTs = beginTs;
        this.commitTs = commitTs;
        this.xid = xid;
        this.next = next;
    }

    public Tuple data() {
        return data;
    }

    public long getBeginTs() {
        return beginTs;
    }

    public long getCommitTs() {
        return commitTs;
    }

    public Version getNext() {
        return next;
    }

    public TupleId getTupleId() {
        return data.id();
    }

    public void setNext(Version next) {
        this.next = next;
    }

    public void setCommitTs(long commitTs) {
        this.commitTs = commitTs;
    }
}