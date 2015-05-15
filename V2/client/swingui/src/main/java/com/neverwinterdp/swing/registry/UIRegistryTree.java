package com.neverwinterdp.swing.registry;

import java.awt.BorderLayout;
import java.awt.event.ActionEvent;
import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import javax.swing.AbstractAction;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JToolBar;
import javax.swing.SwingWorker;
import javax.swing.border.EmptyBorder;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeModel;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.UIMain;
import com.neverwinterdp.swing.UIWorkspace;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.SwingUtil;
import com.neverwinterdp.swing.widget.LazyLoadJTree;
import com.neverwinterdp.swing.widget.LazyLoadJTree.LazyLoadTreeNode;

@SuppressWarnings("serial")
public class UIRegistryTree extends JPanel implements UILifecycle {
  private RegistryNodeSelector registryTree ;
  private String rootPath  = "/";
  private String rootName  = "/";
  
  public UIRegistryTree(String rootPath, String rootName) {
    setLayout(new BorderLayout());
    setOpaque(false);
    setBorder(new EmptyBorder(0, 0, 5, 0) );
    this.rootPath = rootPath ;
    this.rootName = rootName ;
  }
  
  public String getRootPath() { return this.rootPath; }
  
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

    DefaultTreeModel model = new DefaultTreeModel(new RegistryTreeNode(rootPath, rootName));
    registryTree = new RegistryNodeSelector(model);
    registryTree.setShowsRootHandles(true);
    add(new JScrollPane(registryTree), BorderLayout.CENTER);
  }

  public void onDeactivate() {
    removeAll();
  }
  
  public void onSelect(RegistryTreeNode node) {
    UIMain uiScribengin = SwingUtil.findAncestorOfType(this, UIMain.class) ;
    UIWorkspace uiWorkspace = uiScribengin.getUiWorkspace();
    UIRegistryNodeView view = new UIRegistryNodeView(node.getNodePath(), node.getNodeName()) ;
    onCustomNodeView(node, view);
    uiWorkspace.addTab(view.getLabel(), view, true);
  }
  
  protected void onCustomNodeView(RegistryTreeNode node, UIRegistryNodeView view) {
  }
  
  public RegistryTreeNode onCustomTreeNode(RegistryTreeNode node) {
    return node ;
  }
  
  public class RegistryNodeSelector extends LazyLoadJTree {
    public RegistryNodeSelector(final DefaultTreeModel model) throws Exception {
      super(model);
    }
    
    public void onSelect(DefaultMutableTreeNode node) {
      UIRegistryTree.this.onSelect((RegistryTreeNode) node) ;
    }
  }
  
  public class RegistryTreeNode extends LazyLoadTreeNode {
    private static final long serialVersionUID = 1L;
    private String nodePath;
    private String nodeName ;
    
    public RegistryTreeNode(String path, String nodeName) {
      super(nodeName) ;
      this.nodePath = path;
      this.nodeName = nodeName;
    }

    public String getNodePath() { return this.nodePath ; }
    
    public String getNodeName() { return this.nodeName ; }
    
    protected void loadChildren(final DefaultTreeModel model, final PropertyChangeListener progressListener) {
      final Registry registry = Cluster.getCurrentInstance().getRegistry();
      if(registry == null || !registry.isConnect()) {
        System.out.println("INFO: The client is not connected to any registry server");
        return ;
      }
      try {
      if(!registry.exists(nodePath)) {
        return ;
      }
      } catch(Exception ex) {
        ex.printStackTrace();
        return;
      }
      SwingWorker<List<RegistryTreeNode>, Void> worker = new SwingWorker<List<RegistryTreeNode>, Void>() {
        @Override
        protected List<RegistryTreeNode> doInBackground() throws Exception {
          setProgress(0);
          List<String> childrenNames = registry.getChildren(nodePath) ;
          List<RegistryTreeNode> children = new ArrayList<RegistryTreeNode>();
          int size = childrenNames.size();
          for (int i = 0; i < size; i++) {
            String childName = childrenNames.get(i) ;
            String childPath = null ;
            if(nodePath.equals("/")) childPath = "/" + childName ;
            else childPath = nodePath + "/" + childName;
            RegistryTreeNode nodeItem = new RegistryTreeNode(childPath, childName) ; 
            nodeItem = onCustomTreeNode(nodeItem);
            if(nodeItem != null) children.add(nodeItem);
            setProgress((i + 1)/size * 100);
          }
          setProgress(0);
          return children;
        }

        @Override
        protected void done() {
          try {
            setChildren(get());
            model.nodeStructureChanged(RegistryTreeNode.this);
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
  
  static public class RegistryTreeNodePathMatcher {
    private List<Pattern> ignorePathPatterns = new ArrayList<>() ;
    
    public void add(String pathRegex) {
      Pattern pattern = Pattern.compile(pathRegex);
      ignorePathPatterns.add(pattern);
    }
    
    public boolean matches(RegistryTreeNode node) {
      String path = node.getNodePath();
      for(int i = 0; i < ignorePathPatterns.size(); i++) {
        Pattern pattern = ignorePathPatterns.get(i);
        if(pattern.matcher(path).matches()) {
          return true;
        }
      }
      return false ;
    }
  }
}
