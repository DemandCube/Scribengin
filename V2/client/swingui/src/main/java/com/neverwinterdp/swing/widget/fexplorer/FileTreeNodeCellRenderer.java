package com.neverwinterdp.swing.widget.fexplorer;

import java.awt.Component;
import java.io.File;
import java.util.HashMap;
import java.util.Map;

import javax.swing.Icon;
import javax.swing.JLabel;
import javax.swing.JTree;
import javax.swing.tree.DefaultTreeCellRenderer;

class FileTreeNodeCellRenderer extends DefaultTreeCellRenderer {
	private Map<String, Icon> iconCache = new HashMap<String, Icon>();

	private Map<File, String> rootNameCache = new HashMap<File, String>();

	public Component getTreeCellRendererComponent(JTree tree, Object value,
			                                          boolean sel, boolean expanded, 
			                                          boolean leaf, int row, boolean hasFocus) {
		FileTreeNode ftn = (FileTreeNode) value;
		File file = ftn.file;
		String filename = "";
		if (file != null) {
			if (ftn.isFileSystemRoot) {
				filename = this.rootNameCache.get(file);
				if (filename == null) {
					filename = FileTreePanel.SFV.getSystemDisplayName(file);
					this.rootNameCache.put(file, filename);
				}
			} else {
				filename = file.getName();
			}
		}
		JLabel result = 
			(JLabel) super.getTreeCellRendererComponent(tree, filename, sel, expanded, leaf, row, hasFocus);
		if (file != null) {
			Icon icon = this.iconCache.get(filename);
			if (icon == null) {
				icon = FileTreePanel.SFV.getSystemIcon(file);
				this.iconCache.put(filename, icon);
			}
			result.setIcon(icon);
		}
		return result;
	}
}
