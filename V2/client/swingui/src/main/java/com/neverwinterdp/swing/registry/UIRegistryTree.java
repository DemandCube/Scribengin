package com.neverwinterdp.swing.registry;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JPanel;
import javax.swing.JToolBar;
import javax.swing.SwingWorker;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.UIMain;
import com.neverwinterdp.swing.UIWorkspace;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.SwingUtil;
import com.neverwinterdp.swing.widget.JTreeItemSelector;
import com.neverwinterdp.swing.widget.JTreeItemSelector.TreeNodeItem;

@SuppressWarnings("serial")
public class UIRegistryTree extends JPanel implements UILifecycle {
  private RegistryNodeSelector tree ;
  
  public UIRegistryTree() throws Exception {
    setLayout(new BorderLayout());
  }
  
  @Override
  public void onInit() {
  }

  @Override
  public void onDestroy() {
  }
  
  public void onActivate() throws Exception {
    removeAll();
    JToolBar toolbar = new JToolBar();
    toolbar.setFloatable(false);
    toolbar.setRollover(true);
    toolbar.setAutoscrolls(true);
    toolbar.add(new AbstractAction("Reload") {
      public void actionPerformed(ActionEvent e) {
      }
    });
    add(toolbar, BorderLayout.NORTH);

    System.out.println("UIRegistry onActivate(), vmClient = " + Cluster.getInstance().getVMClient());
    DefaultTreeModel model = new DefaultTreeModel(new RegistryNodeItem("/", "/"));
    tree = new RegistryNodeSelector(model);
    tree.setShowsRootHandles(true);
    add(tree, BorderLayout.CENTER);
  }

  public void onDeactivate() {
    removeAll();
  }
  
  static public class RegistryNodeSelector extends JTreeItemSelector {
    public RegistryNodeSelector(final DefaultTreeModel model) throws Exception {
      super(model);
    }
    
    public void onSelect(DefaultMutableTreeNode node) {
      RegistryNodeItem selectNode = (RegistryNodeItem) node ;
      UIMain uiScribengin = SwingUtil.findAncestorOfType(this, UIMain.class) ;
      UIWorkspace uiWorkspace = uiScribengin.getUiWorkspace();
      uiWorkspace.addTab("registry[" + selectNode.getNodeName() + "]", new UIRegistryNodeTextView(selectNode.getNodePath()), true);
    }
  }
  
  static public class RegistryNodeItem extends TreeNodeItem {
    private static final long serialVersionUID = 1L;
    private String nodePath;
    private String nodeName ;
    
    public RegistryNodeItem(String path, String nodeName) {
      super(nodeName) ;
      this.nodePath = path;
      this.nodeName = nodeName;
    }

    public String getNodePath() { return this.nodePath ; }
    
    public String getNodeName() { return this.nodeName ; }
    
    protected void loadChildren(final DefaultTreeModel model, final PropertyChangeListener progressListener) {
      //do not check loaded, force reload the children every time
      //if (loaded) return; 
      final Registry registry = Cluster.getInstance().getVMClient().getRegistry();
      if(registry == null) {
        System.out.println("INFO: The client is not connected to any registry server");
        return ;
      }
      SwingWorker<List<RegistryNodeItem>, Void> worker = new SwingWorker<List<RegistryNodeItem>, Void>() {
        @Override
        protected List<RegistryNodeItem> doInBackground() throws Exception {
          setProgress(0);
          List<String> childrenNames = registry.getChildren(nodePath) ;
          List<RegistryNodeItem> children = new ArrayList<RegistryNodeItem>();
          int size = childrenNames.size();
          for (int i = 0; i < size; i++) {
            String childName = childrenNames.get(i) ;
            String childPath = null ;
            if(nodePath.equals("/")) childPath = "/" + childName ;
            else childPath = nodePath + "/" + childName;
            children.add(new RegistryNodeItem(childPath, childName));
            setProgress((i + 1)/size * 100);
          }
          setProgress(0);
          return children;
        }

        @Override
        protected void done() {
          try {
            setChildren(get());
            model.nodeStructureChanged(RegistryNodeItem.this);
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
}
