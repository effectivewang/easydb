package com.easydb.core;
public record Lock(LockMode mode, Transaction txn) { 
    
}