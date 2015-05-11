package com.neverwinterdp.swing.vm;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.tool.EmbeddedCluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.util.text.StringUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.vm.VMConfig;
import com.neverwinterdp.vm.VMDescriptor;
import com.neverwinterdp.vm.client.VMClient;

@SuppressWarnings("serial")
public class UIVMListView extends SpringLayoutGridJPanel {
  private String  path ;
  
  public UIVMListView(String path) {
    this.path = path ;
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if(registry == null) {
      initNoConnection() ;
    } else {
      try {
        init(registry) ;
      } catch(Throwable e) {
        MessageUtil.handleError(e);
      }
    }
    makeCompactGrid(); 
  }

  private void initNoConnection() {
    JPanel infoPanel = new JPanel();
    infoPanel.add(new JLabel("No Registry Connection"));
    addRow(infoPanel);
  }
  
  private void init(Registry registry) throws Exception {
    JToolBar toolbar = new JToolBar();
    toolbar.setFloatable(false);
    toolbar.add(new AbstractAction("Reload") {
      @Override
      public void actionPerformed(ActionEvent e) {
      }
    });
    addRow(toolbar) ;

    JXTable jxTable = new JXTable();
    
    jxTable.setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
    VMDescriptorTableModel model = new VMDescriptorTableModel(path);
    jxTable.setModel(model);
    model.loadData();
    
    jxTable.setVisibleRowCount(30);
    jxTable.setVisibleColumnCount(8);
    jxTable.setHorizontalScrollEnabled(true);
    jxTable.setColumnControlVisible(true);

    jxTable.setHighlighters(HighlighterFactory.createSimpleStriping());
    // ...oops! we forgot one
    jxTable.addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
   
    JScrollPane scrollPane = new JScrollPane(jxTable);
    addRow(scrollPane) ;
  }
  

  static class VMDescriptorTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Name", "Roles", "Home", "CPU", "Memory"} ;
    private String path ;
    
    public VMDescriptorTableModel(String path) {
      super(COLUMNS, 0) ;
      this.path = path ;
    }
    
    void loadData() throws Exception {
      VMClient vmClient = Cluster.getCurrentInstance().getVMClient() ;
      
      List<VMDescriptor> vmDescriptors = null ;
      if(path.endsWith("active")) vmDescriptors = vmClient.getActiveVMDescriptors();
      else if(path.endsWith("history")) vmDescriptors = vmClient.getHistoryVMDescriptors();
      else vmDescriptors = vmClient.getAllVMDescriptors();
      
      for(int i = 0; i < vmDescriptors.size(); i++) {
        VMDescriptor vmDescriptor = vmDescriptors.get(i) ;
        VMConfig vmConfig = vmDescriptor.getVmConfig();
        Object[] cells = {
          vmConfig.getName(),
          StringUtil.joinStringCollection(vmConfig.getRoles(), ","),
          vmConfig.getAppHome(), 
          vmConfig.getRequestCpuCores(),
          vmConfig.getRequestMemory()
        };
        addRow(cells);
      }
    }
  }
}