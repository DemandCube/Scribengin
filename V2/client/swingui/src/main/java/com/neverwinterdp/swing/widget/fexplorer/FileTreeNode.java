package com.neverwinterdp.swing.widget.fexplorer;

import java.io.File;
import java.util.Enumeration;
import java.util.NoSuchElementException;

import javax.swing.tree.TreeNode;

public class FileTreeNode implements TreeNode {
  File     file;
  File[]   children;
  TreeNode parent;
  boolean  isFileSystemRoot;

  public FileTreeNode(File file, boolean isFileSystemRoot, TreeNode parent) {
    this.file = file;
    this.isFileSystemRoot = isFileSystemRoot;
    this.parent = parent;
    this.children = this.file.listFiles();
    if (this.children == null) this.children = new File[0];
  }

  public FileTreeNode(File[] children) {
    this.file = null;
    this.parent = null;
    this.children = children;
  }

  public File getFile() { return this.file; }

  public Enumeration<?> children() {
    final int elementCount = this.children.length;
    return new Enumeration<File>() {
      int count = 0;

      public boolean hasMoreElements() {
        return this.count < elementCount;
      }

      public File nextElement() {
        if (this.count < elementCount) { return FileTreeNode.this.children[this.count++]; }
        throw new NoSuchElementException("Vector Enumeration");
      }
    };
  }

  public boolean getAllowsChildren() {
    return file.isDirectory() ;
  }

  public TreeNode getChildAt(int childIndex) {
    return new FileTreeNode(this.children[childIndex], this.parent == null, this);
  }

  public int getChildCount() {
    return this.children.length;
  }

  public int getIndex(TreeNode node) {
    FileTreeNode ftn = (FileTreeNode) node;
    for (int i = 0; i < this.children.length; i++) {
      if (ftn.file.equals(this.children[i])) return i;
    }
    return -1;
  }

  public TreeNode getParent() {
    return this.parent;
  }

  public boolean isLeaf() {
    return (this.getChildCount() == 0);
  }
}
