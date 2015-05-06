console.clear();
function hello(frameui) {
  var panel = frameui.panel("InputForm") ;
  panel.fieldSet("textField", "This is the text field") ;
  panel.fieldSet("textArea", "This is the test area") ;
  panel.buttonClick("JButton");
}

console.println("ScriptRunner " + ScriptRunner) ;
ScriptRunner.require("classpath:script/robot.js");

var robot = new Robot() ;
robot.add("hello", "", hello);
robot.run() ;
robot.report() ;

console.println("After:     " + jvm.getMemoryInfo()) ;

