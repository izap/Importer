<?php

namespace Importer\Reader;

class Sqlite extends Source {
  private $_header = array();
  private $_rows = array();

  public function __construct(&$config) {
    parent::__construct($config);
    $this->load();
  }

  public function header() {
    return $this->_header;
  }

  public function records() {
    return $this->_rows;
  }

  /**
   *
   * return array of rows and columns in the loaded file.
   */

  private function load(){
    try {
      $spreadsheet = new \SpreadsheetReader($this->config->workarea_root.date('Y/m/d'). DIRECTORY_SEPARATOR.
        $this->config->workarea.DIRECTORY_SEPARATOR.$this->config->source_file);
      $this->_header = $spreadsheet->current();
      for($i = 0;  $i<$this->config->sheets_to_process; $i++) {
        $spreadsheet->ChangeSheet($i);
        foreach ($spreadsheet as $key => $Row) {
          if ($key === 1) {
            continue;
          }


          // verify if there are specific columns has been provided to process.
          if(isset($this->config->columns) && sizeof($this->config->columns)){
            if(!isset($column_index_array)) {
              foreach ($this->config->columns as $column_name) {
                $column_index_array[] = array_search($column_name, $this->_header);
              }
            }
            foreach ($Row as $c_index => $cell) {
              foreach($column_index_array as $cia){
                if($cia === $c_index){
                  $cell_value[$this->_header[$c_index]] = trim($cell) ;
                }
              }
            }
          }else{
            foreach ($Row as $c_index => $cell) {
              $cell_value[$this->_header[$c_index]] = trim($cell);
            }
          }

          if(!$this->filter($this->config->filter, $cell_value)){
            continue;
          }
          $this->_rows[] = $cell_value ;
          $cell_value = array();
        }
      }
    }
    catch(Exception $e){
      echo $e->getMessage();
    }
  }

  /*
   * array('image_valid' => 'LOCAL')
   */

  private function filter($arg=array(),&$row = null){
    $return = true;
    foreach($arg as $key => $value){
      if($row[$key] != $value){
        return false;
      }
    }
    return $return;
  }

}