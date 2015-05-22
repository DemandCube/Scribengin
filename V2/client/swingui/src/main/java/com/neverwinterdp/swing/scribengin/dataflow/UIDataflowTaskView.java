package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

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

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskDescriptor;
import com.neverwinterdp.scribengin.storage.StreamDescriptor;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIDataflowTaskView extends SpringLayoutGridJPanel implements UILifecycle {
  private String dataflowPath;
  private DataflowTaskJXTable taskTable;
  private DataflowTaskInfoPanel taskInfo;

  public UIDataflowTaskView(String dataflowPath) {
    this.dataflowPath = dataflowPath;
  }

  @Override
  public void onInit() throws Exception {
  }

  @Override
  public void onDestroy() throws Exception {
  }

  @Override
  public void onActivate() throws Exception {
    System.err.println("UIDataflowTaskView: call activate......................");
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

      taskTable = new DataflowTaskJXTable(DataflowRegistry.getDataflowTaskDescriptors(registry, dataflowPath));
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

  public class DataflowTaskJXTable extends JXTable {
    public DataflowTaskJXTable(List<DataflowTaskDescriptor> taskDescriptors) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowTaskTableModel model = new DataflowTaskTableModel(taskDescriptors);
      setModel(model);
      model.loadData();

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowTaskTableModel model = (DataflowTaskTableModel) getModel();
          DataflowTaskDescriptor descriptor = model.getTaskAndReportAt(getSelectedRow());
          taskInfo.updateTaskInfo(descriptor);
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

    public void updateTaskInfo(DataflowTaskDescriptor descriptor) {
      clear();
      createBorder("Dataflow task Info");
      if (descriptor == null) {
        addRow("Select one of the tasks above to view its details. ");
      } else {
        addRow("Task id: ", descriptor.getTaskId());
        addRow("Status: ", "TODO");
        addRow("Scribe: ", descriptor.getScribe());

        addRow("Registry path:", descriptor.getRegistryPath());

        addRow("Source Type: ", descriptor.getSourceStreamDescriptor().getType());
        for(Entry<String, StreamDescriptor> sinkStream : descriptor.getSinkStreamDescriptors().entrySet()) {
          addRow("Sink Name: ", sinkStream.getKey());
          addRow("Sink Type: ", sinkStream.getValue().getType());
        }
      }
      makeCompactGrid();
      revalidate();
    }
  }
  
  static class DataflowTaskTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Id", "Status", "Scribe" };

    List<DataflowTaskDescriptor> taskDescriptors;

    public DataflowTaskTableModel(List<DataflowTaskDescriptor> taskDescriptors) {
      super(COLUMNS, 0);
      this.taskDescriptors = taskDescriptors;
      Collections.sort(taskDescriptors, DataflowTaskDescriptor.COMPARATOR);
    }

    public DataflowTaskDescriptor getTaskAndReportAt(int selectedRow) {
      return taskDescriptors.get(selectedRow);
    }

    void loadData() throws Exception {
      for(DataflowTaskDescriptor sel : taskDescriptors) {
        Object[] cells = {
            sel.getTaskId(), "TODO", sel.getScribe()
        };
        addRow(cells);
      }
    }
  }
}