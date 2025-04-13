package com.easydb.index;

import com.easydb.storage.Page;
import com.easydb.storage.PageManager;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.List;

/**
 * Manages B-tree pages on disk.
 * Similar to PostgreSQL's BTPage structure.
 */
public class BTreePage {
    private static final int PAGE_SIZE = 8192;  // 8KB pages
    private final PageManager pageManager;
    private final long pageId;
    private byte[] data;
    private boolean dirty;

    public BTreePage(PageManager pageManager, long pageId) {
        this.pageManager = pageManager;
        this.pageId = pageId;
        this.data = new byte[PAGE_SIZE];
        this.dirty = false;
    }

    public void writeNode(BTreeNode node) {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            
            // Write node metadata
            oos.writeBoolean(node.isLeaf());
            oos.writeInt(node.getKeys().size());
            
            // Write keys and values
            for (Comparable key : node.getKeys()) {
                oos.writeObject(key);
            }
            
            for (Object value : node.getValues()) {
                if (node.isLeaf()) {
                    oos.writeObject(value);  // TupleId
                } else {
                    BTreeNode child = (BTreeNode) value;
                    oos.writeLong(child.getPageId());
                }
            }
            
            // Write parent reference
            if (node.getParent() != null) {
                oos.writeLong(node.getParent().getPageId());
                oos.writeInt(node.getParentKeyIndex());
            } else {
                oos.writeLong(-1);
                oos.writeInt(-1);
            }
            
            // Copy to page data
            byte[] nodeData = baos.toByteArray();
            System.arraycopy(nodeData, 0, data, 0, nodeData.length);
            dirty = true;
            
        } catch (IOException e) {
            throw new RuntimeException("Failed to write B-tree node", e);
        }
    }

    public BTreeNode readNode() {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(data);
             ObjectInputStream ois = new ObjectInputStream(bais)) {
            
            // Read node metadata
            boolean isLeaf = ois.readBoolean();
            int numKeys = ois.readInt();
            
            // Create node
            BTreeNode node = new BTreeNode(100, isLeaf);  // TODO: Get order from metadata
            
            // Read keys
            for (int i = 0; i < numKeys; i++) {
                node.getKeys().add((Comparable) ois.readObject());
            }
            
            // Read values
            for (int i = 0; i < numKeys + 1; i++) {
                if (isLeaf) {
                    node.getValues().add(ois.readObject());  // TupleId
                } else {
                    long childPageId = ois.readLong();
                    BTreeNode child = readNodeFromPage(childPageId);
                    node.getValues().add(child);
                }
            }
            
            // Read parent reference
            long parentPageId = ois.readLong();
            int parentKeyIndex = ois.readInt();
            
            if (parentPageId != -1) {
                BTreeNode parent = readNodeFromPage(parentPageId);
                node.setParent(parent, parentKeyIndex);
            }
            
            return node;
            
        } catch (IOException | ClassNotFoundException e) {
            throw new RuntimeException("Failed to read B-tree node", e);
        }
    }

    private BTreeNode readNodeFromPage(long pageId) {
        BTreePage page = new BTreePage(pageManager, pageId);
        pageManager.readPage(pageId, page.data);
        return page.readNode();
    }

    public void flush() {
        if (dirty) {
            pageManager.writePage(pageId, data);
            dirty = false;
        }
    }

    public void free() {
        pageManager.freePage(pageId);
    }

    public long getPageId() {
        return pageId;
    }

    public boolean isDirty() {
        return dirty;
    }
} 