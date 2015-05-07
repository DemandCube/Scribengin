package com.neverwinterdp.swing.widget.fexplorer;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.File;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.border.EmptyBorder;
import javax.swing.filechooser.FileSystemView;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreePath;

public class FileTreePanel extends JPanel {
  final static public FileSystemView SFV = FileSystemView.getFileSystemView();
  private JTree                      tree;
  private JPopupMenu                 popup;
  private Action                     defaultAction;

  public FileTreePanel() {
    this.setLayout(new BorderLayout());
    this.tree = createJTree();
    final JScrollPane jsp = new JScrollPane(this.tree);
    jsp.setBorder(new EmptyBorder(0, 0, 0, 0));
    this.add(jsp, BorderLayout.CENTER);
  }

  private JTree createJTree() {
    popup = new JPopupMenu();
    popup.setPopupSize(new Dimension(100, 50));

    File[] roots = File.listRoots();
    FileTreeNode rootTreeNode = new FileTreeNode(roots);
    JTree tree = new JTree(rootTreeNode);
    tree.setCellRenderer(new FileTreeNodeCellRenderer());
    tree.setRootVisible(false);
    tree.add(popup);
    tree.addMouseListener(new PopupTrigger());
    return tree;
  }
  
  public void expand(String path) {
  }
  
  public FileTreeNode getLastSelectedNode() {
    return (FileTreeNode) tree.getLastSelectedPathComponent();
  }

  public void register(Action action, boolean defaultAction) {
    popup.add(action);
    if (defaultAction) this.defaultAction = action;
  }

  public Action createDeteleAction() {
    return new DeleteAction();
  }

  public Action createRefreshAction() {
    return new RefreshAction();
  }

  public class PopupTrigger extends MouseAdapter {
    public void mouseClicked(MouseEvent e) {
      if (e.getClickCount() > 1 && defaultAction != null) {
        defaultAction.actionPerformed(null);
      }
    }

    public void mouseReleased(MouseEvent e) {
      if (e.isPopupTrigger()) {
        int x = e.getX();
        int y = e.getY();
        TreePath path = tree.getPathForLocation(x, y);
        if (path != null) {
          popup.show(tree, x, y);
        }
      }
    }
  }

  public class DeleteAction extends AbstractAction {
    public DeleteAction() {
      super("Delete");
    }

    public void actionPerformed(ActionEvent e) {
      FileTreeNode select = (FileTreeNode) tree.getLastSelectedPathComponent();
      try {
        if (select.isFileSystemRoot) {
          JOptionPane.showMessageDialog(FileTreePanel.this, "Cannot delete a drive", "Delete File",
                                        JOptionPane.ERROR_MESSAGE);
          return;
        }
        String mesg = "Do you really want to delete the file " + select.file.getName();
        int confirm = JOptionPane.showConfirmDialog(FileTreePanel.this, mesg, "Delete File",
                                                    JOptionPane.WARNING_MESSAGE);
        if (confirm == JOptionPane.OK_OPTION) {
          TreePath selectedPath = tree.getLeadSelectionPath();
          select.file.delete();
          ((DefaultTreeModel) tree.getModel()).reload();
          tree.setSelectionPath(selectedPath);
        }
      } catch (Exception ex) {
        JOptionPane.showMessageDialog(FileTreePanel.this, ex.getMessage(), "Delete File", JOptionPane.ERROR_MESSAGE);
      }
    }
  }

  public class RefreshAction extends AbstractAction {
    public RefreshAction() {
      super("Refresh");
    }

    public void actionPerformed(ActionEvent e) {
      TreePath selectedPath = tree.getLeadSelectionPath();
      ((DefaultTreeModel) tree.getModel()).reload();
      tree.setSelectionPath(selectedPath);
    }
  }
}