package com.neverwinterdp.swing.registry;

import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

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
import com.neverwinterdp.registry.activity.Activity;
import com.neverwinterdp.registry.activity.ActivityStep;
import com.neverwinterdp.swing.UILifecycle;
import com.neverwinterdp.swing.scribengin.ScribenginCluster;
import com.neverwinterdp.swing.widget.SpringLayoutGridJPanel;

@SuppressWarnings("serial")
public class UIActivityView extends SpringLayoutGridJPanel implements UILifecycle {
  private String activityPath;
  private DataflowActivityJXTable activityTable;
  private DataflowActivityInfoPanel activityInfo;

  //This should take a nodepath to filter the type of activity?
  public UIActivityView(String activityPath) {
    this.activityPath = activityPath;
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
    Registry registry = ScribenginCluster.getCurrentInstance().getRegistry();
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
      addRow(toolbar);

      activityTable = new DataflowActivityJXTable(getActivities(registry));
      activityInfo = new DataflowActivityInfoPanel();
      JSplitPane splitPane =
          new JSplitPane(JSplitPane.VERTICAL_SPLIT, new JScrollPane(activityTable), new JScrollPane(activityInfo));
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

  protected List<ActivityAndSteps> getActivities(Registry registry) throws RegistryException {
    List<ActivityAndSteps> activities = new ArrayList<>();
    if (!registry.exists(activityPath + "/all")) {
      JPanel infoPanel = new JPanel();
      infoPanel.add(new JLabel("Path: " + activityPath + "/all does not exist!"));
      addRow(infoPanel);
      return new ArrayList<ActivityAndSteps>();
    }
    for (String id : registry.getChildren(activityPath + "/all")) {
      activities.add(
          new ActivityAndSteps(
              registry.getDataAs(activityPath + "/all/" + id, Activity.class),
              registry.getChildrenAs(activityPath + "/all/" + id + "/activity-steps", ActivityStep.class)
          ));

    }
    return activities;
  }

  public class DataflowActivityJXTable extends JXTable {
    public DataflowActivityJXTable(List<ActivityAndSteps> activitiesAndSteps) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      DataflowActivityTableModel model = new DataflowActivityTableModel(activitiesAndSteps);
      setModel(model);
      model.loadData();

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          DataflowActivityTableModel model = (DataflowActivityTableModel) getModel();
          ActivityAndSteps selectedAcivityAndSteps = model.getActivityAndStepsAt(getSelectedRow());
          activityInfo.updateActivityInfo(selectedAcivityAndSteps);
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    }
  }

  public class DataflowActivityInfoPanel extends SpringLayoutGridJPanel {
    public DataflowActivityInfoPanel() {
      updateActivityInfo(null);
    }

    public void updateActivityInfo(ActivityAndSteps activityAndSteps) {
      String indent = "    ";
      clear();
      createBorder("Dataflow Activity Info");
      if (activityAndSteps == null) {
        addRow("Select one of the activities above to view its details. ");
      } else {
        addRow("Activity:",indent, indent);
        addRow(indent, "Activity id: ", activityAndSteps.getId());
        addRow(indent, "Description", activityAndSteps.activity.getDescription());
        addRow(indent, "Step builder: ", activityAndSteps.activity.getActivityStepBuilder());
        addRow(indent, "Coordinator", activityAndSteps.activity.getCoordinator());
        addRow(indent, "Type: ", activityAndSteps.activity.getType());
        addRow("Activity Steps:",indent, indent);
        for (ActivityStep sinkStream : activityAndSteps.getActivitySteps()) {
          addRow(indent, "Description: ", sinkStream.getDescription());
          addRow(indent, "Activity Type: ", sinkStream.getType());
          addRow(indent, "Status: ", sinkStream.getStatus().toString());
          addRow(indent,indent,indent);
        }
      }
      makeCompactGrid();
      revalidate();
    }
  }

  static class DataflowActivityTableModel extends DefaultTableModel {
    static String[] COLUMNS = { "Id", "description", "type" };

    List<ActivityAndSteps> activityAndSteps;

    public DataflowActivityTableModel(List<ActivityAndSteps> activityAndSteps) {
      super(COLUMNS, 0);
      this.activityAndSteps = activityAndSteps;
      Collections.sort(this.activityAndSteps);
    }

    public ActivityAndSteps getActivityAndStepsAt(int selectedRow) {
      return activityAndSteps.get(selectedRow);
    }

    void loadData() throws Exception {
      for (ActivityAndSteps act : activityAndSteps) {
        Activity activity = act.getActivity();
        Object[] cells = {
            activity.getId(), activity.getDescription(), activity.getType() };
        addRow(cells);
      }
    }
  }

  //just a helper
  public class ActivityAndSteps implements Comparable<ActivityAndSteps> {
    private Activity activity;
    private List<ActivityStep> activitySteps;

    public ActivityAndSteps(Activity activity, List<ActivityStep> activitySteps) {
      super();
      this.activity = activity;
      this.activitySteps = activitySteps;
    }

    public Activity getActivity() {
      return activity;
    }

    public void setActivity(Activity activity) {
      this.activity = activity;
    }

    public List<ActivityStep> getActivitySteps() {
      return activitySteps;
    }

    public void setActivitySteps(List<ActivityStep> activitySteps) {
      this.activitySteps = activitySteps;
    }

    public String getId() {
      return activity.getId();
    }

    @Override
    public String toString() {
      StringBuilder builder = new StringBuilder();
      builder.append("ActivityAndSteps [activity=");
      builder.append(activity);
      builder.append(", activitySteps=");
      builder.append(activitySteps);
      builder.append("]");
      return builder.toString();
    }

    @Override
    public int compareTo(ActivityAndSteps other) {
      return this.getActivity().getId().compareToIgnoreCase(other.getActivity().getId());
    }
  }
}
