<?php

namespace Importer;

class Config {

  private $_attributes;

  public function __construct($runtime_config = array()) {
    $this->_attributes = $runtime_config;
    $this->workarea_root = __DIR__.DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.
      'workarea'.DIRECTORY_SEPARATOR;
  }

  public function __get($property) {
    return isset($this->_attributes[$property])?$this->_attributes[$property]:false;
  }

  public function __isset($property){
    return isset($this->_attributes[$property])?true:false;
  }

  public function __unset($property){
     unset($this->_attributes[$property]);
  }

  public function process(){
    $global_config = json_decode(file_get_contents($this->workarea_root.'config.json'), true);
    $specific_config = json_decode(file_get_contents($this->workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.
      $this->workarea.DIRECTORY_SEPARATOR.'config.json'), true);
    $this->_attributes = array_merge((array)$global_config, (array) $specific_config, (array)$this->_attributes);
  }

  public function setup(){
    // create workarea.
    if(@mkdir($this->workarea_root,0755,true)) {
      $global_config_data = '{
    "reader":{"driver": "spreadsheet"},
    "writer":{"driver": "sqlite"},
    "skip_columns": ["image_processed","errors","moiz_comments","record_processed","created_on","updated_on"]
}';
      if(!file_exists($this->workarea_root . 'config.json')) {
        file_put_contents($this->workarea_root . 'config.json', $global_config_data);
      }
    }

    //create workaread as per current time
    $local_file_path = $this->workarea_root . date("Y/m/d/H_i").DIRECTORY_SEPARATOR;
    if(@mkdir($local_file_path,0755,true)){
      $local_config_data = '{
    "source_file": "<csv,xls,xlsx file name>",
    "keyfield": "username",
    "sheets_to_process": "1",
    "table_name": "<table name in sqlite db>",
    "sqlite_db_file": "<sqlite_db_file_name>"
}
';
      if(!file_exists($local_file_path . 'config.json')) {
        file_put_contents($local_file_path . 'config.json', $local_config_data);
      }
    }
    return $local_file_path . 'config.json';
  }

}