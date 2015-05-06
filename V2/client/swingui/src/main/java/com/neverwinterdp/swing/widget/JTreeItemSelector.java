package com.neverwinterdp.swing.widget;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.SwingWorker;
import javax.swing.UIManager;
import javax.swing.event.TreeExpansionEvent;
import javax.swing.event.TreeSelectionEvent;
import javax.swing.event.TreeSelectionListener;
import javax.swing.event.TreeWillExpandListener;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.ExpandVetoException;
import javax.swing.tree.MutableTreeNode;
import javax.swing.tree.TreePath;

public class JTreeItemSelector  extends JTree {
  private static final long serialVersionUID = 1L;

  public JTreeItemSelector(DefaultMutableTreeNode root) throws Exception {
    this(new DefaultTreeModel(root)) ;
  }
  
  public JTreeItemSelector(final DefaultTreeModel model) throws Exception {
    setModel(model) ;
    this.addTreeSelectionListener(new  TreeSelectionListener() {
      public void valueChanged(TreeSelectionEvent evt) {
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) JTreeItemSelector.this.getLastSelectedPathComponent();
        if (node == null) return;
        onSelect(node) ;
      }
    }) ;
    final JProgressBar bar = new JProgressBar();
    final PropertyChangeListener progressListener = new PropertyChangeListener() {
      @Override
      public void propertyChange(PropertyChangeEvent evt) {
        bar.setValue((Integer) evt.getNewValue());
      }
    };

    addTreeWillExpandListener(new TreeWillExpandListener() {
      @Override
      public void treeWillExpand(TreeExpansionEvent event) throws ExpandVetoException {
        TreePath path = event.getPath();
        if (path.getLastPathComponent() instanceof TreeNodeItem) {
          TreeNodeItem node = (TreeNodeItem) path.getLastPathComponent();
          node.loadChildren(model, progressListener);
        }
      }

      @Override
      public void treeWillCollapse(TreeExpansionEvent evt) throws ExpandVetoException {
      }
    });
    TreeNodeItem root = (TreeNodeItem) model.getRoot() ;
    root.loadChildren(model, progressListener);
  }

  public void onSelect(DefaultMutableTreeNode node) {
    System.out.println("Select " + node.getUserObject()) ;
  }

  static public class TreeNodeItem extends DefaultMutableTreeNode {
    private static final long serialVersionUID = 1L;

    protected boolean loaded = false;

    public TreeNodeItem(Object userObj) {
      add(new DefaultMutableTreeNode("Loading...", false));
      setAllowsChildren(true);
      setUserObject(userObj);
    }

    protected void setChildren(List<? extends TreeNodeItem> children) {
      removeAllChildren();
      setAllowsChildren(children.size() > 0);
      for (MutableTreeNode node : children) {
        add(node);
      }
      loaded = true;
    }
    
    protected void loadChildren(final DefaultTreeModel model, final PropertyChangeListener progressListener) {
      if (loaded) return;
      SwingWorker<List<TreeNodeItem>, Void> worker = new SwingWorker<List<TreeNodeItem>, Void>() {
        @Override
        protected List<TreeNodeItem> doInBackground() throws Exception {
          setProgress(0);
          List<TreeNodeItem> children = new ArrayList<TreeNodeItem>();
          for (int i = 0; i < 5; i++) {
            // Simulate DB access time
            Thread.sleep(100);
            children.add(new TreeNodeItem("Child " + i + " at level " + 1));
            setProgress((i + 1) * 20);
          }
          setProgress(0);
          return children;
        }

        @Override
        protected void done() {
          try {
            setChildren(get());
            model.nodeStructureChanged(TreeNodeItem.this);
          } catch (Exception e) {
            e.printStackTrace();
          }
          super.done();
        }
      };
      
      if (progressListener != null) {
        worker.getPropertyChangeSupport().addPropertyChangeListener("progress", progressListener);
      }
      worker.execute();
    }
  }
  
  //--------------------------------------TEST------------------------------------------------
  
  public static void main(String[] args) throws Exception {
    final DefaultTreeModel model = new DefaultTreeModel(new TreeNodeItem("Root"));
    JTreeItemSelector tree = new JTreeItemSelector(model);
    tree.setShowsRootHandles(true);
    
    JFrame frame = new JFrame("Creating a Simple JTree");
    frame.addWindowListener(new WindowAdapter() {
      public void windowClosing(WindowEvent event) {
        System.exit(0);
      }
    });
    
    UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
    Container content = frame.getContentPane();
    content.add(new JScrollPane(tree), BorderLayout.CENTER);
    frame.setSize(275, 300);
    frame.setVisible(true);
  }
}