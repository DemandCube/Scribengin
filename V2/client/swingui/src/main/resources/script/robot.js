importPackage(java.lang) ;
importPackage(com.neverwinterdp.swing.robot) ;

function Robot() {
  this.units = [] ;
  
  this.add = function(name, description, method) {
    var unit = {name: name, description: description, method: method } ;
    this.units.push(unit) ;
  };
  
  this.run = function() {
    for(var i = 0; i < this.units.length; i++) {
      var unit = this.units[i] ;
      unit.status = '' ;
    }
    var frameui = new FrameUI(uiroot) ;
    for(var i = 0; i < this.units.length; i++) {
      var unit = this.units[i] ;
      unit.method(frameui) ;
      unit.status = 'OK' ;
      Thread.sleep(500) ;
    }
    frameui.destroy() ;
  };
  
  this.report = function() {
    var header = ["Name", "Status"] ;
    var width  = [30,     10] ;
    console.println("") ;
    console.println('-----------------------------------------') ;
    console.println(header, width) ;
    console.println('-----------------------------------------') ;
    for(var i = 0; i < this.units.length; i++) {
      var unit = this.units[i] ;
      console.println([unit.name, unit.status], width) ;
    }
  };
}
