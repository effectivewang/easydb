package com.easydb.index;

import java.util.concurrent.atomic.AtomicReferenceArray;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.List;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;

/**
 * Lock-free Bloom Filter implementation
 */
public class BloomFilter<T> {
    private final AtomicReferenceArray<Boolean> bits;
    private final int size;
    private final List<Function<T, Integer>> hashFunctions;
    private final AtomicInteger elementCount;
    
    public BloomFilter(int size, int numHashes) {
        this.size = size;
        this.bits = new AtomicReferenceArray<>(size);
        this.hashFunctions = new ArrayList<>();
        this.elementCount = new AtomicInteger(0);
        
        for (int i = 0; i < numHashes; i++) {
            final int seed = i;
            hashFunctions.add(item -> Math.abs((item.hashCode() + seed) % size));
        }
    }
    
    public void add(T item) {
        boolean added = false;
        for (Function<T, Integer> hashFunction : hashFunctions) {
            int index = hashFunction.apply(item);
            if (bits.compareAndSet(index, null, true)) {
                added = true;
            }
        }
        if (added) {
            elementCount.incrementAndGet();
        }
    }
    
    public boolean mightContain(T item) {
        for (Function<T, Integer> hashFunction : hashFunctions) {
            int index = hashFunction.apply(item);
            if (bits.get(index) == null || !bits.get(index)) {
                return false;
            }
        }
        return true;
    }
    
    public double getFalsePositiveRate() {
        double k = hashFunctions.size();
        double n = elementCount.get();
        double m = size;
        return Math.pow(1 - Math.exp(-k * n / m), k);
    }

    public void clear() {
        for (int i = 0; i < size; i++) {
            bits.set(i, null);
        }
        elementCount.set(0);
    }
}