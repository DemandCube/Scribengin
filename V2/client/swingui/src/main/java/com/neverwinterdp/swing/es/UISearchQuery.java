package com.neverwinterdp.swing.es;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.JButton;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.jdesktop.swingx.JXTable;
import org.jdesktop.swingx.decorator.ColorHighlighter;
import org.jdesktop.swingx.decorator.HighlightPredicate;
import org.jdesktop.swingx.decorator.HighlighterFactory;

import com.neverwinterdp.es.ESClient;
import com.neverwinterdp.es.ESObjectClient;
import com.neverwinterdp.es.log4j.Log4jRecord;
import com.neverwinterdp.swing.util.MessageUtil;
import com.neverwinterdp.swing.widget.BeanBindingJTextField;

@SuppressWarnings("serial")
public class UISearchQuery extends JPanel {
  String index;
  UISearchQueryPlugin plugin ;
  QueryPanel queryPanel ;
  UISearchQueryResult queryResultPanel ;
  
  public UISearchQuery(String index, UISearchQueryPlugin plugin) throws Exception {
    this.index  = index ;
    this.plugin = plugin;
    
    queryPanel       = new QueryPanel() ;
    queryResultPanel = new UISearchQueryResult(plugin) ;
    
    setLayout(new BorderLayout());
    add(queryPanel, BorderLayout.NORTH) ;
    add(new JScrollPane(queryResultPanel), BorderLayout.CENTER) ;
    plugin.onInit(this);
  }
  
  public JXTable getUISearchQueryResult() { return this.queryResultPanel; }
  
  public class QueryPanel extends JPanel {
    private String query ;
    
    public QueryPanel() {
      setLayout(new BorderLayout());
      
      Action action = new AbstractAction("Search") {
        @Override
        public void actionPerformed(ActionEvent e) {
          queryResultPanel.update(query);
        }
      };
      BeanBindingJTextField<QueryPanel> queryField = new BeanBindingJTextField<>(this, "query", true);
      add(queryField, BorderLayout.CENTER);
      add(new JButton(action), BorderLayout.EAST) ;
    }

    public String getQuery()  { return query; }
    public void setQuery(String query) { this.query = query; }
  }
  
  public class UISearchQueryResult extends JXTable {
    public UISearchQueryResult(UISearchQueryPlugin plugin) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      SearchResultTableModel model = new SearchResultTableModel(plugin);
      setModel(model);
      model.loadData();

      setVisibleRowCount(30);
      setVisibleColumnCount(8);
      setHorizontalScrollEnabled(true);
      setColumnControlVisible(true);
      addMouseListener(new MouseAdapter() {
        public void mouseClicked(MouseEvent e) {
          SearchResultTableModel model = (SearchResultTableModel) getModel();
        }
      });
      setHighlighters(HighlighterFactory.createSimpleStriping());
      addHighlighter(new ColorHighlighter(HighlightPredicate.ROLLOVER_ROW, Color.BLACK, Color.WHITE));
    }
    
    public void update(String query) {
      SearchResultTableModel model = (SearchResultTableModel) getModel();
      try {
        model.update(plugin, index, query);
      } catch (Exception e) {
        MessageUtil.handleError(e);;
      }
    }
  }
  
  @SuppressWarnings("serial")
  static public class SearchResultTableModel extends DefaultTableModel {
    private List<Object[]> rows ;
    
    public SearchResultTableModel(UISearchQueryPlugin plugin) {
      super(plugin.getColumNames(), 0);
    }

    public void update(UISearchQueryPlugin plugin, String index, String query) throws Exception {
      ESClient esclient = ESCluster.getInstance().getESClient();
      this.rows = plugin.query(esclient, index, query);
      getDataVector().clear();
      loadData();
      fireTableDataChanged();
    }
    
    void loadData() throws Exception {
      if(rows == null) return;
      for(int i = 0; i < rows.size(); i++) {
        addRow(rows.get(i));
      }
    }
  }
}
