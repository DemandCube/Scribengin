package com.neverwinterdp.swing.widget;

import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Insets;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;
import java.beans.Expression;
import java.beans.IntrospectionException;
import java.beans.PropertyDescriptor;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.swing.JButton;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;

import com.neverwinterdp.swing.util.BeanInspector;

abstract public class BeanBindingJTable<T> extends JTable {
  private static final long serialVersionUID = 1L;

  private JPopupMenu rowPopupMenu;
  protected List<T> beans;
  protected List<Expression> expressions = new ArrayList<Expression>();
  private String[] beanProperty;
  protected String[] columNames;
  protected Class[] columnType;
  BeanInspector<T> beanInspector;
  private boolean edit;
  private DateFormat df = new SimpleDateFormat("dd/MM/yyyy");

  public BeanBindingJTable() {
  }

  public BeanBindingJTable(String[] columns, String[] beanProperty, List<T> beanList) {
    init(columns, beanProperty, beanList);
  }

  protected void init(String[] columns, String[] beanProperty, List<T> beanList) {
    this.columNames = columns;
    this.beanProperty = beanProperty;
    this.beans = beanList;

    T sampleBean = newBean();
    beanInspector = BeanInspector.get(newBean().getClass());

    try {
      columnType = new Class[beanProperty.length];
      for (int i = 0; i < columnType.length; i++) {
        PropertyDescriptor descriptor = new PropertyDescriptor(beanProperty[i], sampleBean.getClass());
        columnType[i] = descriptor.getReadMethod().getReturnType();
        if (columnType[i] == boolean.class)
          columnType[i] = Boolean.class;
        else if (columnType[i] == int.class)
          columnType[i] = Integer.class;
        else if (columnType[i] == long.class)
          columnType[i] = Long.class;
        else if (columnType[i] == double.class)
          columnType[i] = Double.class;
        else if (columnType[i] == Date.class)
          columnType[i] = String.class;
        else if (columnType[i] == float.class)
          columnType[i] = Float.class;
      }
    } catch (IntrospectionException e) {
      throw new RuntimeException(e);
    }

    final AbstractTableModel model = new AbstractTableModel() {
      public Class getColumnClass(int column) {
        return getBeanPropertyClass(column);
      }

      public String getColumnName(int column) {
        return columNames[column];
      }

      public int getRowCount() {
        return beans.size();
      }

      public int getColumnCount() {
        return columNames.length;
      }

      public Object getValueAt(int rowIndex, int columnIndex) {
        return getBeanValueAt(rowIndex, columnIndex);
      }

      public boolean isCellEditable(int row, int col) {
        return isBeanEditableAt(row, col);
      }

      public void setValueAt(Object aValue, int row, int col) {
        setBeanValueAt(aValue, row, col);
      }
    };
    setModel(model);
  }

  abstract protected T newBean();

  public Class getBeanPropertyClass(int column) {
    return columnType[column];
  }

  abstract protected boolean isBeanEditableAt(int row, int col);

  public Object getBeanValueAt(int row, int column) {
    if (column == 0) {
      return row + 1;
    } else {
      T bean = beans.get(row);
      String property = beanProperty[column];
      if (property.toLowerCase().indexOf("date") >= 0) {
        try {
         return df.format(beanInspector.getValue(bean, property));
        } catch (Exception e) {
          return beanInspector.getValue(bean, property);
        }

      } else {
        return beanInspector.getValue(bean, property);
      }

    }

  }

  public void setBeanValueAt(Object value, int row, int column) {
    T bean = beans.get(row);
    String property = beanProperty[column];
    beanInspector.setValue(bean, property, value);
    onChangeBeanData(row, bean, property, value);
    fireTableDataChanged();
  }

  public T getSelectedBean() {
    int row = getSelectedRow();
    return beans.get(row);
  }

  public void onChangeBeanData(int row, T bean, String property, Object val) {
  }

  public boolean onAddRow() {
    return false;
  }

  public boolean onRemoveRow(T bean, int row) {
    return true;
  }

  public void fireTableDataChanged() {
    AbstractTableModel model = (AbstractTableModel) getModel();
    model.fireTableDataChanged();
  }

  public void addBean(T bean) {
    beans.add(bean);
    fireTableDataChanged();
  }

  public void removeBeanAt(int idx) {
    T bean = beans.get(idx);
    if (onRemoveRow(bean, idx)) {
      beans.remove(idx);
      fireTableDataChanged();
    }
  }

  public List<T> getBeans() {
    return this.beans;
  }

  public void setBeans(List<T> beans) {
    this.beans = beans;
    this.fireTableDataChanged();
  }

  public JPopupMenu createRowPopupMenu() {
    rowPopupMenu = new JPopupMenu();
    MouseListener popupListener = new MouseAdapter() {
      public void mouseReleased(MouseEvent e) {
        if (SwingUtilities.isRightMouseButton(e)) {
          rowPopupMenu.show(e.getComponent(), e.getX(), e.getY());
        }
      }
    };
    addMouseListener(popupListener);

    JMenuItem addField = new JMenuItem("Add Row");
    addField.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        if (onAddRow())
          fireTableDataChanged();
      }
    });
    rowPopupMenu.add(addField);

    JMenuItem delField = new JMenuItem("Delete Row");
    delField.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        int row = getSelectedRow();
        T bean = beans.get(row);
        if (onRemoveRow(bean, row)) {
          beans.remove(row);
          fireTableDataChanged();
        }
      }
    });
    rowPopupMenu.add(delField);

    JMenuItem moveUp = new JMenuItem("Move Up");
    moveUp.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        int row = getSelectedRow();
        if (row > 0) {
          T bean = beans.remove(row);
          beans.add(row - 1, bean);
          fireTableDataChanged();
        }
      }
    });
    rowPopupMenu.add(moveUp);

    JMenuItem moveDown = new JMenuItem("Move Down");
    moveDown.addActionListener(new ActionListener() {
      public void actionPerformed(ActionEvent e) {
        int row = getSelectedRow();
        if (row < beans.size() - 1) {
          T bean = beans.remove(row);
          beans.add(row + 1, bean);
          fireTableDataChanged();
        }
      }
    });
    rowPopupMenu.add(moveDown);
    return rowPopupMenu;
  }

  public JPanel getPanleButton() {
    JPanel panel2 = new JPanel();
    panel2.setOpaque(false);
    panel2.setSize(95, 50);
    panel2.setPreferredSize(new Dimension(95, 50));
    panel2.setLayout(new FlowLayout(FlowLayout.RIGHT));
    JButton btn = new JButton("<html> <p>/\\<p>/\\</p></p></html>");
    btn.setName("btnTop");
    btn.addActionListener(new ActionListener() {

      @Override
      public void actionPerformed(ActionEvent e) {
        int row = getSelectedRow();
        if (row > 0) {
          edit = true;
          beans.add(0, beans.remove(row));
          fireTableDataChanged();
        }

      }
    });
    btn.setPreferredSize(new Dimension(94, 50));
    panel2.add(btn);
    JButton btn1 = new JButton("/\\");
    btn1.setName("btnUp");
    btn1.addActionListener(new ActionListener() {

      @Override
      public void actionPerformed(ActionEvent e) {
        int row = getSelectedRow();
        if (row > 0) {
          edit = true;
          beans.add(row - 1, beans.remove(row));
          fireTableDataChanged();
        }

      }
    });
    btn1.setPreferredSize(new Dimension(94, 50));
    panel2.add(btn1);
    JButton btn2 = new JButton("\\/");
    btn2.setName("btnDown");
    btn2.addActionListener(new ActionListener() {

      @Override
      public void actionPerformed(ActionEvent e) {
        int row = getSelectedRow();
        if (row < beans.size() - 1) {
          edit = true;
          beans.add(row + 1, beans.remove(row));
          fireTableDataChanged();
        }
      }
    });
    btn2.setPreferredSize(new Dimension(94, 50));
    panel2.add(btn2);
    JButton btn3 = new JButton("<html> <p>\\/<p>\\/</p></p></html>");
    btn3.addActionListener(new ActionListener() {

      @Override
      public void actionPerformed(ActionEvent e) {
        int row = getSelectedRow();
        if (row < beans.size() - 1) {
          edit = true;
          beans.add(beans.size() - 1, beans.remove(row));
          fireTableDataChanged();
        }

      }
    });
    btn3.setName("btnBottom");
    btn3.setPreferredSize(new Dimension(94, 50));
    panel2.add(btn3);
    JButton btn4 = new JButton("Update");
    btn4.setMargin(new Insets(0, 0, 0, 0));
    btn4.addActionListener(new ActionListener() {
      @Override
      public void actionPerformed(ActionEvent e) {
        edit = false;
        saveIndex();
      }
    });
    btn4.setPreferredSize(new Dimension(94, 30));
    panel2.add(btn4);
    return panel2;
  }

  public void saveIndex() {
  }

  public boolean isEdit() {
    return edit;
  }

}