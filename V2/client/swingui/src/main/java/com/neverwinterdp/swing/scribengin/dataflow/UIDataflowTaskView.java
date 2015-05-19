package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import javax.swing.AbstractAction;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.registry.RegistryException;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.util.text.DateUtil;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIDataflowTaskView extends SpringLayoutGridJPanel implements UILifecycle {
  private String tasksPath;
  private DataflowTasksJXTable taskTable;
  private DataflowTaskInfoPanel taskInfo;

  public UIDataflowTaskView(String tasksPath) {
    this.tasksPath = tasksPath;
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    clear();
    Registry registry = Cluster.getCurrentInstance().getRegistry();
    if (registry == null) {
      addRow("No Registry Connection");
    } else {
      JToolBar toolbar = new JToolBar();
      toolbar.setFloatable(false);
      toolbar.add(new AbstractAction("Reload") {
       //TODO make it work
        @Override
        public void actionPerformed(ActionEvent e) {
        }
      });
      addRow(toolbar);

      taskTable = new DataflowTasksJXTable(getTasks(registry));
      taskInfo = new DataflowTaskInfoPanel();
      JSplitPane splitPane =
          new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(taskTable), new JScrollPane(taskInfo));
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

  protected List<TaskAndReport> getTasks(Registry registry) throws RegistryException {
    List<TaskAndReport> tasksAndReports = new ArrayList<>();
    if (!registry.exists(tasksPath + "/descriptors")) {
      JPanel infoPanel = new JPanel();
      infoPanel.add(new JLabel("Path: " + tasksPath + "/descriptors does not exist!"));
      addRow(infoPanel);
      return new ArrayList<TaskAndReport>();
    }
    for (String id : registry.getChildren(tasksPath + "/descriptors")) {
      tasksAndReports.add(
          new TaskAndReport(id,
              registry.getDataAs(tasksPath + "/descriptors/" + id, DataflowTaskDescriptor.class),
              registry.getDataAs(tasksPath + "/descriptors/" + id + "/report", DataflowTaskReport.class)
          ));
    }
    return tasksAndReports;
  }

  private static String getValidTime(long time) {
    if (time == 0l)
      return "-";
    else
      return DateUtil.asCompactDateTime(time);
 }

  public class DataflowTasksJXTable extends JXTable {
    public DataflowTasksJXTable(List<TaskAndReport> tasksAndReports) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowTaskTableModel model = new DataflowTaskTableModel(tasksAndReports);
      setModel(model);
      model.loadData();

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowTaskTableModel model = (DataflowTaskTableModel) getModel();
          TaskAndReport selectedTaskReport = model.getTaskAndReportAt(getSelectedRow());
          System.err.println("Ime bonyezwa.");
          taskInfo.updateTaskInfo(selectedTaskReport);
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));

    }
  }

  public class DataflowTaskInfoPanel extends SpringLayoutGridJPanel {
    public DataflowTaskInfoPanel() {
      updateTaskInfo(null);
    }

    public void updateTaskInfo(TaskAndReport taskAndReport) {
      String indent = "    ";
      clear();
      createBorder("Dataflow task Info");
      if (taskAndReport == null) {
        addRow("Select one of the tasks above to view its details. ");
      } else {
        System.err.println("report " + taskAndReport.getTaskDescriptor().getSourceStreamDescriptor());
        addRow(indent, "Task id: ", taskAndReport.getId());
        addRow(indent, "Status: ", taskAndReport.getTaskDescriptor().getStatus().toString());
        addRow(indent, "Scribe: ", taskAndReport.getTaskDescriptor().getScribe().toString());

        addRow(indent, "Start time:", getValidTime(taskAndReport.getReport().getStartTime()));
        addRow(indent, "Finish time:", getValidTime(taskAndReport.getReport().getFinishTime()));
        addRow(indent, "Process count:", taskAndReport.getReport().getProcessCount());
        addRow(indent, "Commit count:", taskAndReport.getReport().getCommitProcessCount());
        addRow(indent, "Registry path:",taskAndReport.getTaskDescriptor().getRegistryPath());

        addRow(indent,"Source Type: ", taskAndReport.getTaskDescriptor().getSourceStreamDescriptor().getType());
        for (Entry<String, StreamDescriptor> sinkStream : taskAndReport.getTaskDescriptor().getSinkStreamDescriptors()
            .entrySet()) {
          addRow(indent,  "Sink Name: ", sinkStream.getKey());
          addRow(indent,  "Sink Type: ", sinkStream.getValue().getType());
        }
      }
      makeCompactGrid();
      revalidate();
    }
  }
  
  static class DataflowTaskTableModel extends DefaultTableModel {
    static String[] COLUMNS = {
        "Id", "Status", "Process Count", "Commit Process Count", "Start Time", "Finish Time" };

    List<TaskAndReport> tasksAndReports;

    public DataflowTaskTableModel(List<TaskAndReport> tasksAndReports) {
      super(COLUMNS, 0);
      this.tasksAndReports = tasksAndReports;
      Collections.sort(this.tasksAndReports);
    }

    public TaskAndReport getTaskAndReportAt(int selectedRow) {

      return tasksAndReports.get(selectedRow);
    }

    void loadData() throws Exception {
      for (TaskAndReport tar : tasksAndReports) {
        DataflowTaskReport report = tar.getReport();
        DataflowTaskDescriptor desc = tar.getTaskDescriptor();

        Object[] cells = {
            tar.getId(), desc.getStatus(), report.getProcessCount(),
            report.getCommitProcessCount(), getValidTime(report.getStartTime()),getValidTime(report.getFinishTime())
        };
        addRow(cells);
      }
    }
  }

  //Simple class to help map taskDescriptor with its Report and ID
  public class TaskAndReport implements Comparable<TaskAndReport> {
    public String id;
    public DataflowTaskDescriptor taskDescriptor;
    public DataflowTaskReport report;

    public TaskAndReport(String ID, DataflowTaskDescriptor dataflowTaskDesc, DataflowTaskReport report) {
      this.id = ID;
      this.taskDescriptor = dataflowTaskDesc;
      this.report = report;
    }

    public String getId() {
      return id;
    }

    public DataflowTaskReport getReport() {
      return report;
    }

    public DataflowTaskDescriptor getTaskDescriptor() {
      return taskDescriptor;
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("TaskAndReport [id=");
      builder.append(id);
      builder.append(", taskDescriptor=");
      builder.append(taskDescriptor);
      builder.append(", report=");
      builder.append(report);
      builder.append("]");
      return builder.toString();
    }

    @Override
    public int compareTo(TaskAndReport other) {
      return this.getId().compareToIgnoreCase(other.getId());
    }
  }
}
