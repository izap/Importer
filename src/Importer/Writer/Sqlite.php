<?php

namespace Importer\Writer;

class Sqlite extends Destination {

  public function __construct(&$config){
    parent::__construct($config);
  }

  public function insert(&$records) {

    //todo: put exception for records empty and header empty;

    $dbObject = new \SQLite3($this->config->workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.
      $this->config->workarea.DIRECTORY_SEPARATOR.$this->config->sqlite_db_file);

    $insert_command = 'INSERT INTO ' . $this->config->table_name. '(%s) VALUES (%s); ' ;
    $build_command = null;

    $header = true;
    foreach ($records as $row_index => $row){
      if($row_index === 0 ) {continue;}
      foreach ($row as $rkey => $cell) {
        //skip columns which you do not want to be in update process.
        if (in_array($rkey, $this->config->skip_columns) || empty($rkey)){ continue; }
        $update_columns[] = "'".trim($dbObject->escapeString($cell))."'";
        if($header){
          $header_array[]=$rkey;
        }
      }
      $header = false;

      //todo: put exception keyfield is important
      $build_command .= sprintf($insert_command, implode(", ", $header_array) ,  implode(", ", $update_columns));


      if(isset($this->config->debug) && $this->config->debug == 'yes'){
        echo $build_command;
        return true;
      }
      $update_columns= array();
    }
    return $dbObject->exec($build_command);
  }

  /**
   * @param $records  array of all rows to be imported in sqlite db.
   * @return bool return exec result.
   */

  public function update(&$records){
    $dbObject = new \SQLite3($this->config->workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.
      $this->config->workarea.DIRECTORY_SEPARATOR.$this->config->sqlite_db_file);

    $build_command = null;
    foreach ($records as $row_index => $row){

      if($row_index === 0 ) {continue;}

      foreach ($row as $rkey => $cell) {
        //skip columns which you do not want to be in update process.
        if (in_array($rkey, $this->config->skip_columns) || empty($rkey) || $rkey==$this->config->keyfield){ continue; }
        $update_columns[] = $rkey."='".$dbObject->escapeString($cell)."'";
      }

      //todo: process metadata columns.
      if(isset($this->config->metadata)) {
        foreach ($this->config->metadata as $mvalue) {
          $update_columns[] = $mvalue;
        }
      }

      $update_columns[] = 'updated_on=datetime("now")';

      //todo: put exception keyfield is important


      $build_command .= 'UPDATE ' . $this->config->table_name. ' SET '.implode(", ", $update_columns).
                       ' WHERE '. $this->config->keyfield."='".$row['username']."'; ";
      if(isset($this->config->debug) && $this->config->debug == 'yes'){
        echo $build_command;
        return true;
      }
      $update_columns= array();
    }

    if(isset($this->config->pre_query) && !empty($this->config->pre_query)){
      $dbObject->exec($this->config->pre_query);
    }
    $output = $dbObject->exec($build_command);

    if(isset($this->config->post_query) && !empty($this->config->post_query)){
      $dbObject->exec($this->config->post_query);
    }

    return $output;
  }

  public function create_table($columns){
    $dbObject = new \SQLite3($this->config->workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.
      $this->config->workarea.DIRECTORY_SEPARATOR.$this->config->sqlite_db_file);
    $build_command = null;

    foreach ($columns as $column_index => $column_name) {
      $c_name = implode('_', explode(' ', strtolower($column_name)));
      $create_table_fields[] = $c_name . ' TEXT';
    }
    $create_table_fields[] = 'record_processed TEXT DEFAULT N';
    $create_table_fields[] = 'updated_on DATETIME';
    $create_table_fields[] = 'created_on DATETIME';



    $create_table_fields_string = implode(', ', $create_table_fields);
    $create_table_statement = 'CREATE TABLE if not exists ' . $this->config->table_name . ' (' . $create_table_fields_string . ');';

    if(isset($this->config->debug) && $this->config->debug == 'yes'){
      echo $create_table_statement;
      return true;
    }

    if($dbObject->exec($create_table_statement)){
      $create_index = "CREATE INDEX {$this->config->keyfield}_index ON {$this->config->table_name} ({$this->config->keyfield});";
      return $dbObject->exec($create_index);
    }
    return false;
  }

}