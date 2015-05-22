package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Node;
import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIDataflowWorkerView extends SpringLayoutGridJPanel implements UILifecycle {
  private String workersPath;
  private DataflowWorkersJXTable workerTable;
  private DataflowWorkerInfoPanel workerInfo;

  public UIDataflowWorkerView(String workersPath) {
    this.workersPath = workersPath;
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    System.err.println("UIDataflowWorkerView: call activate......................");
    clear();
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if (registry == null) {
      addRow("No Registry Connection");
    } else {
      JToolBar toolbar = new JToolBar();
      toolbar.setFloatable(false);
      toolbar.add(new AbstractAction("Reload") {
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
      toolbar.add(new AbstractAction("All Workers") {
        @Override
        public void actionPerformed(ActionEvent e) {
          onChangeWorkerListPath(workersPath + "/all");
        }
      });
      toolbar.add(new AbstractAction("Active Workers") {
        @Override
        public void actionPerformed(ActionEvent e) {
          onChangeWorkerListPath(workersPath + "/active");
        }
      });
      toolbar.add(new AbstractAction("History Workers") {
        @Override
        public void actionPerformed(ActionEvent e) {
          onChangeWorkerListPath(workersPath + "/history");
        }
      });
      addRow(toolbar);

      workerTable = new DataflowWorkersJXTable(workersPath + "/all", workersPath + "/all");
      workerInfo = new DataflowWorkerInfoPanel();
      JSplitPane splitPane =
          new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(workerTable), new JScrollPane(workerInfo));
      splitPane.setOneTouchExpandable(true);
      splitPane.setDividerLocation(150);

      addRow(splitPane);
    }
    makeCompactGrid();
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }

  public void onChangeWorkerListPath(String listPath) {
    try {
      DataflowWorkersTableModel model = (DataflowWorkersTableModel) workerTable.getModel();
      model.setWorkerListPath(listPath);
    } catch (Exception e) {
      MessageUtil.handleError(e);
    }
  }

  public class DataflowWorkersJXTable extends JXTable {
    public DataflowWorkersJXTable(String workerAllPath, String workerListPath) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowWorkersTableModel model = new DataflowWorkersTableModel(workerAllPath, workerListPath);
      setModel(model);
      model.loadData();

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowWorkersTableModel model = (DataflowWorkersTableModel) getModel();
          DataflowWorkerInfo selectedWorkerInfo = model.getDataflowWorkerInfoAt(getSelectedRow());
          workerInfo.updateWorkerInfo(selectedWorkerInfo);
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    }
  }

  public class DataflowWorkerInfoPanel extends SpringLayoutGridJPanel {
    public DataflowWorkerInfoPanel() {
      updateWorkerInfo(null);
    }

    public void updateWorkerInfo(DataflowWorkerInfo selectedWorkerInfo) {
      String indent= "    ";
      clear();
      createBorder("Dataflow Worker Info");
      if (selectedWorkerInfo == null) {
        addRow("Select one of the workers above to view its details. ");
      } else {
        Registry registry = Cluster.getCurrentInstance().getRegistry();
        addRow(indent,"Worker id: ", selectedWorkerInfo.getWorkerId());
        addRow(indent,"Worker path: ", selectedWorkerInfo.getDataflowWorkerPath());
        addRow(indent,"Status:", selectedWorkerInfo.getStatus());

        //TODO: load the dataflow worker descriptor, show it in the 
        //TODO (tuan) what dataflow worker descriptor?
        try {
          Node data = registry.get(selectedWorkerInfo.getDataflowWorkerPath());
          int numExecutors = data.getChild("executors").getChildren().size();        
          addRow(indent,"Executor count: ", numExecutors);
        } catch (RegistryException e) {

        }
      }
      makeCompactGrid();
      revalidate();
    }
  }

  static class DataflowWorkersTableModel extends DefaultTableModel {
    static String[] COLUMNS = {
        "Worker ID", "Worker Status",
    };

    private String workerAllPath;
    private String workerListPath;
    private List<DataflowWorkerInfo> workerInfos;

    public DataflowWorkersTableModel(String workerAllPath, String workerListPath) {
      super(COLUMNS, 0);
      this.workerAllPath = workerAllPath;
      this.workerListPath = workerListPath;
    }

    public void setWorkerListPath(String listPath) throws Exception {
      this.workerListPath = listPath;
      getDataVector().clear();
      loadData();
      fireTableDataChanged();
    }

    public DataflowWorkerInfo getDataflowWorkerInfoAt(int row) {
      return workerInfos.get(row);
    }

    void loadData() throws Exception {
      workerInfos = loadWorkerInfos();
      for (DataflowWorkerInfo workerInfo : workerInfos) {
        Object[] cell = {
            workerInfo.getWorkerId(),
            workerInfo.getStatus(),
        };
        addRow(cell);
      }
    }

    List<DataflowWorkerInfo> loadWorkerInfos() throws Exception {
      Registry registry = Cluster.getCurrentInstance().getRegistry();
      List<DataflowWorkerInfo> workerInfos = new ArrayList<>();
      for (String workerId : registry.getChildren(workerListPath)) {
        String status = new String(registry.getData(workerAllPath + "/" + workerId + "/status"));
        workerInfos.add(new DataflowWorkerInfo(workerAllPath + "/" + workerId, workerId, status));
      }
      Collections.sort(workerInfos);
      return workerInfos;
    }
  }

  static public class DataflowWorkerInfo implements Comparable<DataflowWorkerInfo>{
    private String dataflowWorkerPath;
    public String workerId;
    public String status;

    public DataflowWorkerInfo(String dataflowWorkerPath, String workerId, String status) {
      this.dataflowWorkerPath = dataflowWorkerPath;
      this.workerId = workerId;
      this.status = status;
    }

    public String getDataflowWorkerPath() {
      return this.dataflowWorkerPath;
    }

    public String getWorkerId() {
      return this.workerId;
    }

    public String getStatus() {
      return this.status;
    }

    @Override
    public int compareTo(DataflowWorkerInfo other) {
      return this.workerId.compareToIgnoreCase(other.workerId);
    }
  }
}