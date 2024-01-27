// This function will run when the spreadsheet is opened
function onOpen() {
    var ui = SpreadsheetApp.getUi();
    
    // Create a new menu
    ui.createMenu('ZI Automation')
      // Add a menu item that runs the cloneSheets function when clicked
      .addItem('Create New ZI Sheet', 'cloneSheets')
      .addItem('Delete ZI Sheets', 'deleteSheets')
      // Add the menu to the UI
      .addToUi();
  }

function cloneSheets() {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    var templateSheet = ss.getSheetByName('ZI Template');
    var sourceSheet = ss.getSheetByName('ZI Source');
    
    // Get the 'ZI Type' column values
    var ziTypeValues = sourceSheet.getRange('F2:F' + sourceSheet.getLastRow()).getValues();
    
    // Create a JavaScript Set to store distinct 'ZI Type' values
    var distinctZiTypes = new Set();
    
    // Add each 'ZI Type' value to the Set
    ziTypeValues.forEach(function(row) {
      distinctZiTypes.add(row[0]);
    });
    
    // For each distinct 'ZI Type' value, clone the template sheet and rename it
    distinctZiTypes.forEach(function(ziType) {
        if (ziType !== '') {  // Ignore empty strings
            
            var newSheetName = 'ZI Template - ' + ziType;
            var existingSheet = ss.getSheetByName(newSheetName);
            
            if (!existingSheet) {
                var newSheet = templateSheet.copyTo(ss);
                newSheet.setName(newSheetName);

                // Edit the Google Sheets query in a specific cell to include the name of the sheet
                var cell = newSheet.getRange('A1');  // TODO: Update this with the actual cell
                var query = '=QUERY(\'ZI Source\'!A:F, "SELECT * WHERE F = \'' + ziType + '\'")';
                cell.setValue(query);
            }
        }
    });
  }

function deleteSheets() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var sourceSheet = ss.getSheetByName('Zi Source');
  
  // Get the 'ZI Type' column values
  var ziTypeValues = sourceSheet.getRange('F2:F' + sourceSheet.getLastRow()).getValues();
  
  // Create a JavaScript Set to store distinct 'ZI Type' values
  var distinctZiTypes = new Set();
  
  // Add each 'ZI Type' value to the Set
  ziTypeValues.forEach(function(row) {
    distinctZiTypes.add(row[0]);
  });
  
  // For each distinct 'ZI Type' value, delete the corresponding sheet
  distinctZiTypes.forEach(function(ziType) {
    if (ziType !== '') {  // Ignore empty strings
      var sheetName = 'ZI Template - ' + ziType;
      var sheet = ss.getSheetByName(sheetName);
      
      if (sheet) {  // If the sheet exists
        ss.deleteSheet(sheet);
      }
    }
  });
}