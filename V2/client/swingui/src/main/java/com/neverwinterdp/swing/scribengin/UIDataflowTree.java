package com.neverwinterdp.swing.scribengin;

import java.awt.BorderLayout;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JPanel;
import javax.swing.SwingWorker;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.service.ScribenginService;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.UIMain;
import com.neverwinterdp.swing.UIWorkspace;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.SwingUtil;
import com.neverwinterdp.swing.widget.LazyLoadJTree;
import com.neverwinterdp.swing.widget.LazyLoadJTree.LazyLoadTreeNode;

@SuppressWarnings("serial")
public class UIDataflowTree extends JPanel implements UILifecycle {
  private RegistryNodeSelector tree ;
  
  public UIDataflowTree() throws Exception {
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
    DefaultTreeModel model = new DefaultTreeModel(new NodeItem(ScribenginService.DATAFLOWS_PATH, "Dataflows"));
    tree = new RegistryNodeSelector(model);
    tree.setShowsRootHandles(true);
    add(tree, BorderLayout.CENTER);
  }

  public void onDeactivate() {
    removeAll();
  }
  
  static public class RegistryNodeSelector extends LazyLoadJTree {
    public RegistryNodeSelector(final DefaultTreeModel model) throws Exception {
      super(model);
    }
    
    public void onSelect(DefaultMutableTreeNode node) {
      NodeItem selectNode = (NodeItem) node ;
      UIMain uiScribengin = SwingUtil.findAncestorOfType(this, UIMain.class) ;
      UIWorkspace uiWorkspace = uiScribengin.getUiWorkspace();
    }
  }
  
  static public class NodeItem extends LazyLoadTreeNode {
    private static final long serialVersionUID = 1L;
    
    static public enum NodeType {UNKNOWN, DATAFLOW_LIST, DATAFLOW, DATAFLOW_COMPONENT}
    
    private String nodePath;
    private String nodeName ;
    private NodeType nodeType = NodeType.UNKNOWN;

    public NodeItem(String path, String nodeName) {
      super(nodeName) ;
      this.nodePath = path;
      this.nodeName = nodeName;
    }

    
    public NodeItem(String path, String nodeName, NodeType nodeType) {
      this(path, nodeName) ;
      this.nodeType = nodeType;
    }

    public String getNodePath() { return this.nodePath ; }
    
    public String getNodeName() { return this.nodeName ; }
    
    public NodeType getNodeType() { return this.nodeType ; }
    
    protected void loadChildren(final DefaultTreeModel model, final PropertyChangeListener progressListener) {
      //if (loaded) return; 
      final Registry registry = Cluster.getInstance().getRegistry();
      if(registry == null) return ;
      SwingWorker<List<NodeItem>, Void> worker = new SwingWorker<List<NodeItem>, Void>() {
        @Override
        protected List<NodeItem> doInBackground() throws Exception {
          if(nodePath.equals(ScribenginService.DATAFLOWS_PATH)) {
            return loadRoot();
          } else if(nodeType == NodeType.DATAFLOW_LIST) {
            return loadDataflowList();
          } else if(nodeType == NodeType.DATAFLOW) {
            return loadVMServices();
          }
          throw new Exception("Unknown node type, path = " + nodePath);
        }
        
        protected List<NodeItem> loadDataflowList() throws Exception {
          List<NodeItem> children = new ArrayList<NodeItem>();
          List<String> childrenNames = registry.getChildren(nodePath) ;
          int size = childrenNames.size();
          for (int i = 0; i < size; i++) {
            String childName = childrenNames.get(i) ;
            String childPath = nodePath + "/" + childName;
            NodeItem nodeItem = new NodeItem(childPath, childName, NodeType.DATAFLOW) ;
            children.add(nodeItem);
          }
          return children;
        }
        
        protected List<NodeItem> loadVMServices() throws Exception {
          List<NodeItem> children = new ArrayList<NodeItem>();
          String[] serviceNames = { "jmx" } ;
          for(int i = 0; i < serviceNames.length; i++) {
            String name = serviceNames[i] ;
            String path = nodePath + "/" + name ;
            NodeItem serviceNode = new NodeItem(path, name, NodeType.DATAFLOW_COMPONENT); 
            serviceNode.setAllowsChildren(false);
            children.add(serviceNode);
          }
          return children;
        }
        
        protected List<NodeItem> loadRoot() throws Exception {
          List<NodeItem> children = new ArrayList<NodeItem>();
          children.add(new NodeItem(ScribenginService.DATAFLOWS_ACTIVE_PATH, "Active", NodeType.DATAFLOW_LIST));
          children.add(new NodeItem(ScribenginService.DATAFLOWS_HISTORY_PATH, "History", NodeType.DATAFLOW_LIST));
          children.add(new NodeItem(ScribenginService.DATAFLOWS_ALL_PATH, "All", NodeType.DATAFLOW_LIST));
          return children;
        }

        
        @Override
        protected void done() {
          try {
            setChildren(get());
            model.nodeStructureChanged(NodeItem.this);
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
