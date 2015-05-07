package com.neverwinterdp.swing.swingx;

import java.awt.*;
import java.awt.event.*;
import javax.swing.*;
import javax.swing.border.*;

public class LayeredPaneWithOverlap {

  private JTextArea    textArea  = new JTextArea(2, 10);
  private JPanel       textPanel = new JPanel(new BorderLayout());
  private JTable       table     = new JTable(30, 5);
  private JScrollPane  scroll    = new JScrollPane(table);
  private JLayeredPane layer     = new JLayeredPane();
  private JFrame       frame     = new JFrame("Frame with resiziable JLayeredPane");

  public void makeUI() {
    textArea.setBorder(new LineBorder(Color.DARK_GRAY));
    textArea.setText("Frame with resiziable JLayeredPane");
    textPanel.setOpaque(false);
    textPanel.add(textArea, BorderLayout.NORTH);
    textPanel.setPreferredSize(new Dimension(500, 200)) ;
    
    Font font = textArea.getFont();
    FontMetrics fontMetrics = textArea.getFontMetrics(font);
    int h = 
        fontMetrics.getHeight() + frame.getInsets().top +
        textPanel.getInsets().top + textArea.getInsets().top + 
        textArea.getInsets().bottom;
    scroll.setBounds(0, h, 400, 300);
    
    layer.add(textPanel, new Integer(2));
    layer.add(scroll, new Integer(1));
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.setSize(600, 400);
    frame.addComponentListener(new ComponentAdapter() {

      @Override
      public void componentResized(ComponentEvent e) {

        SwingUtilities.invokeLater(new Runnable() {

          @Override
          public void run() {
            resizeAll();
          }
        });
      }
    });
    frame.setLocationRelativeTo(null);
    frame.add(layer);
    resizeAll();
    frame.setVisible(true);
  }

  void resizeAll() {
    Insets insets = frame.getInsets();
    int w = frame.getWidth() - insets.left - insets.right;
    int h = frame.getHeight() - insets.top - insets.bottom;
    textPanel.setSize(w, h);
    scroll.setSize(w, h - scroll.getY());
    layer.revalidate();
    layer.repaint();
  }

  public static void main(String[] args) {
    SwingUtilities.invokeLater(new Runnable() {

      @Override
      public void run() {
        new LayeredPaneWithOverlap().makeUI();
      }
    });
  }
}