package com.neverwinterdp.swing.scribengin.dataflow;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.Collections;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JScrollPane;
import javax.swing.JToolBar;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.registry.Registry;
import com.neverwinterdp.scribengin.dataflow.DataflowRegistry;
import com.neverwinterdp.scribengin.dataflow.DataflowTaskReport;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.tool.Cluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;
import com.neverwinterdp.util.text.DateUtil;

@SuppressWarnings("serial")
public class UIDataflowTaskReportView extends SpringLayoutGridJPanel implements UILifecycle {
  private String dataflowPath;
  private DataflowTaskReportJXTable taskReportTable;

  public UIDataflowTaskReportView(String dataflowPath) {
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

      taskReportTable = new DataflowTaskReportJXTable(DataflowRegistry.getDataflowTaskReports(registry, dataflowPath));
      addRow(new JScrollPane(taskReportTable));
    }
    makeCompactGrid();
  }

  @Override
  public void onDeactivate() throws Exception {
    clear();
  }

  public class DataflowTaskReportJXTable extends JXTable {
    public DataflowTaskReportJXTable(List<DataflowTaskReport> reports) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowTaskReportTableModel model = new DataflowTaskReportTableModel(reports);
      setModel(model);
      model.loadData();

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowTaskReportTableModel model = (DataflowTaskReportTableModel) getModel();
          DataflowTaskReport report = model.getDataflowTaskReportAt(getSelectedRow());
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));

    }
  }

  static class DataflowTaskReportTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Id", "Process", "Commit", "Start Time", "Finish Time"};

    List<DataflowTaskReport> reports;

    public DataflowTaskReportTableModel(List<DataflowTaskReport> reports) {
      super(COLUMNS, 0);
      this.reports = reports;
      Collections.sort(reports, DataflowTaskReport.COMPARATOR);
    }

    public DataflowTaskReport getDataflowTaskReportAt(int selectedRow) {
      return reports.get(selectedRow);
    }

    void loadData() throws Exception {
      for(DataflowTaskReport sel : reports) {
        Object[] cells = {
            sel.getTaskId(), sel.getProcessCount(), sel.getCommitProcessCount(), 
            DateUtil.asCompactDateTime(sel.getStartTime()), DateUtil.asCompactDateTime(sel.getFinishTime())
        };
        addRow(cells);
      }
    }
  }
}