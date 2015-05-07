package com.neverwinterdp.swing.widget;

import java.awt.BasicStroke;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Graphics;
import java.awt.Graphics2D;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.awt.event.MouseListener;

import javax.swing.AbstractButton;
import javax.swing.BorderFactory;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.plaf.basic.BasicButtonUI;

public class RemovableJLabel extends JPanel {
  private static final long serialVersionUID = 1L;
  
  private JLabel label ;
  
  public RemovableJLabel(String text) {
    super(new FlowLayout(FlowLayout.LEFT, 0, 0));
    setOpaque(false);

    label = new JLabel(text) ;
    add(label);
    //add more space between the label and the button
    label.setBorder(BorderFactory.createEmptyBorder(0, 0, 0, 5));
    //tab button
    JButton rmButton = new RemoveButton() ;
    add(rmButton);
    //add more space to the top of the component
    setBorder(BorderFactory.createEmptyBorder(2, 0, 0, 0));
  }
  
  protected void onRemove() {
    System.out.println("Remove this label: " + label.getText()) ;
  }

  private class RemoveButton extends JButton implements ActionListener {
    public RemoveButton() {
      int size = 17;
      setPreferredSize(new Dimension(size, size));
      setToolTipText("Remove this label: " + label.getText());
      //Make the button looks the same for all Laf's
      setUI(new BasicButtonUI());
      //Make it transparent
      setContentAreaFilled(false);
      //No need to be focusable
      setFocusable(false);
      setBorder(BorderFactory.createEtchedBorder());
      setBorderPainted(false);
      //Making nice rollover effect
      //we use the same listener for all buttons
      addMouseListener(buttonMouseListener);
      setRolloverEnabled(true);
      //Close the proper tab by clicking the button
      addActionListener(this);
    }

    public void actionPerformed(ActionEvent e) { onRemove() ; }

    //we don't want to update UI for this button
    public void updateUI() { }

    //paint the cross
    protected void paintComponent(Graphics g) {
      super.paintComponent(g);
      Graphics2D g2 = (Graphics2D) g.create();
      //shift the image for pressed buttons
      if (getModel().isPressed()) {
        g2.translate(1, 1);
      }
      g2.setStroke(new BasicStroke(2));
      g2.setColor(Color.BLACK);
      if (getModel().isRollover()) {
        g2.setColor(Color.MAGENTA);
      }
      int delta = 6;
      g2.drawLine(delta, delta, getWidth() - delta - 1, getHeight() - delta - 1);
      g2.drawLine(getWidth() - delta - 1, delta, delta, getHeight() - delta - 1);
      g2.dispose() ;
    }
  }

  private final static MouseListener buttonMouseListener = new MouseAdapter() {
    public void mouseEntered(MouseEvent e) {
      Component component = e.getComponent();
      if (component instanceof AbstractButton) {
        AbstractButton button = (AbstractButton) component;
        button.setBorderPainted(true);
      }
    }

    public void mouseExited(MouseEvent e) {
      Component component = e.getComponent();
      if (component instanceof AbstractButton) {
        AbstractButton button = (AbstractButton) component;
        button.setBorderPainted(false);
      }
    }
  };
  
  static public void main(String[] args) throws Exception {
    JFrame.setDefaultLookAndFeelDecorated(true);
    JFrame frame = new JFrame();
    frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
    frame.add(new RemovableJLabel("test")) ;
    frame.setSize(200, 100);
    frame.setLocationRelativeTo(null);
    frame.setVisible(true) ;
    Thread.currentThread().join() ;
  }
}