package com.neverwinterdp.swing.widget;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.FocusAdapter;
import java.awt.event.FocusEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JTextField;
import javax.swing.SwingUtilities;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.JTextComponent;

import com.neverwinterdp.swing.widget.model.AutoCompleteItem;

abstract public class AutoCompleteJComboBox<T> extends JComboBox<AutoCompleteItem<T>> {
  private static final long serialVersionUID = 1L;

  public AutoCompleteJComboBox() {
    setEditable(true);
    final JTextComponent tc = (JTextComponent) getEditor().getEditorComponent();
    
    tc.getDocument().addDocumentListener(new DocumentListener() {
      public void changedUpdate(DocumentEvent evt) { }

      public void insertUpdate(DocumentEvent evt) { update(); }

      public void removeUpdate(DocumentEvent evt) { update(); }

      public void update() {
        SwingUtilities.invokeLater(new Runnable() {
          public void run() {
            String text = tc.getText();
            Collection<AutoCompleteItem<T>> founds = search(text);
            setEditable(false);
            removeAllItems();
            AutoCompleteItem<T> matchedItem = null ;
            for(AutoCompleteItem<T> sel : founds) {
              if(matches(sel, text)) {
                matchedItem = sel ;
                break ;
              }
            }
            if(matchedItem != null) addItem(matchedItem) ;
            else addItem(new AutoCompleteItem<T>(text, null)) ;

            for(AutoCompleteItem<T> sel : founds) {
              if(matchedItem != sel) addItem(sel);
            }
            setEditable(true);
            setPopupVisible(true);
            requestFocusInWindow();
          }
        });
      }
    });

    tc.addFocusListener(new FocusAdapter() {
      public void focusGained(FocusEvent evt) {
        if (tc.getText().length() > 0) {
          setPopupVisible(true);
        }
      }
    });
    
    addActionListener(new ActionListener () {
      public void actionPerformed(ActionEvent e) {
        AutoCompleteItem<T> selectItem = (AutoCompleteItem<T>)getSelectedItem() ;
        if(selectItem == null) return ;
        if(selectItem.getValue() != null)  {
          tc.setBackground(Color.white) ;
        } else {
          tc.setBackground(Color.red) ;
        }
        onSelectItem(tc, selectItem.getValue()) ;
      }
    });
  }

  public String getText() {
    JTextComponent tc = (JTextComponent) getEditor().getEditorComponent();
    return tc.getText() ;
  }
  
  public void setText(String text) {
    final JTextComponent tc = (JTextComponent) getEditor().getEditorComponent();
    tc.setText(text) ;
  }
  
  protected void onSelectItem(JTextComponent textComponent, T item) {
    System.out.println("Select " + item);
  }
  
  abstract protected Collection<AutoCompleteItem<T>> search(String term) ;

  abstract protected boolean matches(AutoCompleteItem<T> item, String term) ;

  public static void main(String[] args) throws Exception {
    SwingUtilities.invokeAndWait(new Runnable() {
      public void run() {
        AutoCompleteJComboBox<String> combo = new AutoCompleteJComboBox<String>() {
          public Collection<AutoCompleteItem<String>> search(String term) {
            List<AutoCompleteItem<String>> holder = new ArrayList<AutoCompleteItem<String>>();
            holder.add(new AutoCompleteItem<String>("car", "car"));
            holder.add(new AutoCompleteItem<String>("cap", "cap"));
            holder.add(new AutoCompleteItem<String>("cape", "cape"));
            holder.add(new AutoCompleteItem<String>("canadian", "canadian"));
            holder.add(new AutoCompleteItem<String>("caprecious", "caprecious"));
            holder.add(new AutoCompleteItem<String>("catepult", "catepult"));
            return holder; 
          }

          protected boolean matches(AutoCompleteItem<String> item, String term) {
            System.out.println("call matches " + item.getDisplay());
            return item.getValue().equals(term) ;
          }
        };
        JFrame frame = new JFrame();
        frame.setLayout(new BorderLayout()) ;
        frame.add(combo, BorderLayout.NORTH);
        frame.add(new JTextField(), BorderLayout.SOUTH);
        frame.pack();
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        frame.setVisible(true);
      }
    });
  }
}
