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
import javax.swing.ListSelectionModel;
import javax.swing.table.DefaultTableModel;

import org.elasticsearch.action.admin.indices.stats.IndexStats;
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
  private QueryPanel queryPanel ;
  private QueryResultPanel resultPanel ;
  
  public UISearchQuery(IndexStats indexStats) throws Exception {
    setLayout(new BorderLayout());
    queryPanel = new QueryPanel() ;
    resultPanel = new QueryResultPanel(indexStats) ;
    add(queryPanel, BorderLayout.NORTH) ;
    add(resultPanel, BorderLayout.CENTER) ;
  }
  
  public class QueryPanel extends JPanel {
    private String query ;
    
    public QueryPanel() {
      setLayout(new BorderLayout());
      
      Action action = new AbstractAction("Search") {
        @Override
        public void actionPerformed(ActionEvent e) {
          resultPanel.update(query);
        }
      };
      BeanBindingJTextField<QueryPanel> queryField = new BeanBindingJTextField<>(this, "query", true);
      add(queryField, BorderLayout.CENTER);
      add(new JButton(action), BorderLayout.EAST) ;
    }

    public String getQuery()  { return query; }
    public void setQuery(String query) { this.query = query; }
  }
  
  public class QueryResultPanel extends JXTable {
    public QueryResultPanel(IndexStats indexStats) throws Exception {
      setSelectionMode(ListSelectionModel.SINGLE_SELECTION);
      SearchResultTableModel model = new SearchResultTableModel();
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
        model.update(query);
      } catch (Exception e) {
        MessageUtil.handleError(e);;
      }
    }
  }
  
  static class SearchResultTableModel extends DefaultTableModel {
    static String[] COLUMNS = {"Id", "Timestamp", "Level", "Thread", "Name", "Message" };

    private List<Log4jRecord> records ;
    
    public SearchResultTableModel() {
      super(COLUMNS, 0);
    }

    public void update(String query) throws Exception {
      ESClient esclient = ESCluster.getInstance().getESClient();
      ESObjectClient<Log4jRecord> esObjectClient = new ESObjectClient<Log4jRecord>(esclient, "log4j", Log4jRecord.class) ;
      SearchResponse response = esObjectClient.search(QueryBuilders.queryString(query));
      List<Log4jRecord> records = new ArrayList<>();
      SearchHits hits = response.getHits();
      for(SearchHit hit : hits.getHits()) {
        records.add(esObjectClient.getIDocument(hit));
      }
      this.records = records;
      getDataVector().clear();
      loadData();
      fireTableDataChanged();
    }
    
    void loadData() throws Exception {
      if(records == null) return;
      for(int i = 0; i < records.size(); i++) {
        Log4jRecord record = records.get(i);
        Object[] cells = {
          i, record.getTimestamp(), record.getLevel(), record.getThreadName(), record.getLoggerName(), record.getMessage() 
        };
        addRow(cells);
      }
    }
  }
}
