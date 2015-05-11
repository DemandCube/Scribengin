package com.neverwinterdp.swing.widget;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.event.ItemEvent;
import java.awt.event.ItemListener;
import java.awt.event.MouseEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.util.EventObject;

import javax.swing.AbstractCellEditor;
import javax.swing.JCheckBox;
import javax.swing.JFrame;
import javax.swing.JScrollPane;
import javax.swing.JTree;
import javax.swing.UIManager;
import javax.swing.event.ChangeEvent;
import javax.swing.tree.DefaultMutableTreeNode;
import javax.swing.tree.DefaultTreeCellRenderer;
import javax.swing.tree.DefaultTreeModel;
import javax.swing.tree.TreeCellEditor;
import javax.swing.tree.TreeCellRenderer;
import javax.swing.tree.TreeModel;
import javax.swing.tree.TreePath;

public class JTreeCheckBoxNode extends JTree {
  private static final long serialVersionUID = 1L;

  public JTreeCheckBoxNode(TreeModel model) {
    super(model) ;
    setCellRenderer(new CheckBoxNodeRenderer());
    setCellEditor(new CheckBoxNodeEditor(this));
  }
  
  static class CheckBoxNodeRenderer implements TreeCellRenderer {
    private JCheckBox leafRenderer = new JCheckBox();
    private DefaultTreeCellRenderer nonLeafRenderer = new DefaultTreeCellRenderer();

    Color selectionBorderColor, selectionForeground, selectionBackground,
    textForeground, textBackground;

    protected JCheckBox getLeafRenderer() { return leafRenderer; }

    public CheckBoxNodeRenderer() {
      leafRenderer.setFont(UIManager.getFont("Tree.font")) ;
      Boolean booleanValue = (Boolean) UIManager.get("Tree.drawsFocusBorderAroundIcon");
      leafRenderer.setFocusPainted((booleanValue != null) && (booleanValue.booleanValue()));

      selectionBorderColor = UIManager.getColor("Tree.selectionBorderColor");
      selectionForeground = UIManager.getColor("Tree.selectionForeground");
      selectionBackground = UIManager.getColor("Tree.selectionBackground");
      textForeground = UIManager.getColor("Tree.textForeground");
      textBackground = UIManager.getColor("Tree.textBackground");
    }

    public Component getTreeCellRendererComponent(JTree tree, Object value, boolean selected, boolean expanded, 
                                                  boolean leaf, int row, boolean hasFocus) {

      Component returnValue;
      if (leaf) {
        String stringValue = tree.convertValueToText(value, selected, expanded, leaf, row, false);
        leafRenderer.setText(stringValue);
        leafRenderer.setSelected(false);
        leafRenderer.setEnabled(tree.isEnabled());

        if (selected) {
          leafRenderer.setForeground(selectionForeground);
          leafRenderer.setBackground(selectionBackground);
        } else {
          leafRenderer.setForeground(textForeground);
          leafRenderer.setBackground(textBackground);
        }

        if ((value != null) && (value instanceof DefaultMutableTreeNode)) {
          Object userObject = ((DefaultMutableTreeNode) value).getUserObject();
          if (userObject instanceof CheckBoxNode) {
            CheckBoxNode node = (CheckBoxNode) userObject;
            leafRenderer.setText(node.getText());
            leafRenderer.setSelected(node.isSelected());
          }
        }
        returnValue = leafRenderer;
      } else {
        returnValue = nonLeafRenderer.getTreeCellRendererComponent(tree, value, selected, expanded, leaf, row, hasFocus);
      }
      return returnValue;
    }
  }
  
  static class CheckBoxNodeEditor extends AbstractCellEditor implements TreeCellEditor {
    private static final long serialVersionUID = 1L;

    ChangeEvent changeEvent = null;
    JTree tree;

    public CheckBoxNodeEditor(JTree tree) {
      this.tree = tree;
    }

    public Object getCellEditorValue() {
      CheckBoxNodeRenderer renderer = (CheckBoxNodeRenderer)tree.getCellRenderer() ;
      JCheckBox checkbox = renderer.getLeafRenderer();
      CheckBoxNode checkBoxNode = new CheckBoxNode(checkbox.getText(), checkbox.isSelected());
      return checkBoxNode ;
    }

    public boolean isCellEditable(EventObject event) {
      boolean returnValue = false;
      if (event instanceof MouseEvent) {
        MouseEvent mouseEvent = (MouseEvent) event;
        TreePath path = tree.getPathForLocation(mouseEvent.getX(), mouseEvent.getY());
        if (path != null) {
          Object node = path.getLastPathComponent();
          if ((node != null) && (node instanceof DefaultMutableTreeNode)) {
            DefaultMutableTreeNode treeNode = (DefaultMutableTreeNode) node;
            Object userObject = treeNode.getUserObject();
            returnValue = ((treeNode.isLeaf()) && (userObject instanceof CheckBoxNode));
          }
        }
      }
      return returnValue;
    }

    public Component getTreeCellEditorComponent(JTree tree, Object value, boolean selected, 
                                                boolean expanded, boolean leaf, int row) {
      CheckBoxNodeRenderer renderer = (CheckBoxNodeRenderer)tree.getCellRenderer() ;
      Component editor = renderer.getTreeCellRendererComponent(tree, value, true, expanded, leaf, row, true);
      // queryBuilder always selected / focused
      ItemListener itemListener = new ItemListener() {
        public void itemStateChanged(ItemEvent itemEvent) {
          if (stopCellEditing()) {
            fireEditingStopped();
          }
        }
      };
      if (editor instanceof JCheckBox) {
        ((JCheckBox) editor).addItemListener(itemListener);
      }

      return editor;
    }
  }
  

  
  static public class CheckBoxNode {
    String text;
    boolean selected;

    public CheckBoxNode(String text, boolean selected) {
      this.text = text;
      this.selected = selected;
    }

    public boolean isSelected() { return selected; }

    public void setSelected(boolean newValue) { selected = newValue; }

    public String getText() { return text; }
    public void setText(String newValue) { text = newValue; }

    public String toString() { return text; }
  }

  public static void main(String args[]) throws Exception  {
    DefaultMutableTreeNode accessibility = new DefaultMutableTreeNode(new CheckBoxNode("Root", false), true) ;
    accessibility.add(new DefaultMutableTreeNode(new CheckBoxNode("Node 1", false))) ;
    accessibility.add(new DefaultMutableTreeNode(new CheckBoxNode("Node 2", true))) ;
    
    final JTreeCheckBoxNode tree = new JTreeCheckBoxNode(new DefaultTreeModel(accessibility));
    tree.setEditable(true);

    
    JFrame frame = new JFrame("CheckBox Tree");
    frame.addWindowListener(new WindowAdapter() {
      public void windowClosing(WindowEvent event) {
        TreeModel model = tree.getModel() ;
        DefaultMutableTreeNode node = (DefaultMutableTreeNode) model.getRoot() ;
        dump(node, "") ;
        System.exit(0);
      }
      
      private void dump(DefaultMutableTreeNode node, String indent) {
        CheckBoxNode userObj = (CheckBoxNode) node.getUserObject() ;
        System.out.println(indent + userObj.getText() + "[" + userObj.isSelected() + "]");
        for(int i = 0 ; i < node.getChildCount(); i++) {
          dump((DefaultMutableTreeNode)node.getChildAt(i), indent + "  ") ;
        }
      }
    });
    JScrollPane scrollPane = new JScrollPane(tree);
    frame.getContentPane().add(scrollPane, BorderLayout.CENTER);
    frame.setSize(300, 150);
    frame.setVisible(true);
  }
}