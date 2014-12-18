<?php

namespace Importer\Writer;

class Sqlite extends Destination {

  private $dbObject = null;

  public function __construct(&$config){
    parent::__construct($config);
    $this->dbObject = new \SQLite3($this->config->workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.
      $this->config->workarea.DIRECTORY_SEPARATOR.$this->config->sqlite_db_file);
  }

  /**
   * @param $records
   * @return bool
   */
  public function insert(&$records) {
    //todo: put exception for records empty and header empty;
    $insert_command = 'INSERT INTO ' . $this->config->table_name. '(%s) VALUES (%s); ' ;
    $build_command = null;
    $output = array();
    $header = true;
    foreach ($records as $row_index => $row){
      if($row_index === 0 ) {continue;}
      foreach ($row as $rkey => $cell) {
        //skip columns which you do not want to be in update process.
        if (in_array($rkey, $this->config->skip_columns) || empty($rkey)){ continue; }
        $update_columns[] = "'".trim($this->dbObject->escapeString($cell))."'";
        if($header){
          $c_name = implode('_', explode(' ', strtolower($rkey)));
          $header_array[]=$c_name;
        }
      }
      $header = false;

      //todo: put exception keyfield is important

      if(isset($this->config->bulk) && $this->config->bulk == "yes") {
        $build_command .= sprintf($insert_command, implode(", ", $header_array) ,  implode(", ", $update_columns));
      }else {
        $build_command = sprintf($insert_command, implode(", ", $header_array) ,  implode(", ", $update_columns));
      }

      if(!isset($this->config->bulk) or $this->config->bulk != 'yes'){
        if(isset($this->config->debug) && $this->config->debug == 'yes') {
          echo $build_command;
          return true;
        }
        $output[]=$this->dbObject->exec($build_command);
      }

      $update_columns= array();
    }

    if(isset($this->config->bulk) && $this->config->bulk == 'yes'){
      if(isset($this->config->debug) && $this->config->debug == 'yes') {
        echo $build_command;
        return true;
      }
      $output[] = $this->dbObject->exec($build_command);
    }
    return $output;
  }

  /**
   * @return bool
   */
  public function create_index(){
    if(isset($this->config->index_fields)){
      // Create index of desired fields.
      if(is_array($this->config->index_fields) && count($this->config->index_fields)){
        $build_command = '';
        foreach($this->config->index_fields as $indexing_field){
          $build_command .= "CREATE INDEX index_{$this->config->table_name}_{$indexing_field}
                            ON {$this->config->table_name} ({$indexing_field});";
        }
      }

      if(isset($this->config->debug) && $this->config->debug == 'yes'){
        echo $build_command;
        return true;
      }

      return $this->dbObject->exec($build_command);
    }
  }

  /**
   * @param $records  array of all rows to be imported in sqlite db.
   * @return bool return exec result.
   */

  public function update(&$records){
    $build_command = null;
    foreach ($records as $row_index => $row){
      if ($row_index === 0 ) {continue;}

      foreach ($row as $rkey => $cell) {
        //skip columns which you do not want to be in update process.
        if (in_array($rkey, $this->config->skip_columns) || empty($rkey) || $rkey==$this->config->keyfield){ continue; }
        $c_name = implode('_', explode(' ', strtolower($rkey)));
        $update_columns[] = $c_name."='".$this->dbObject->escapeString($cell)."'";
      }

      //todo: process metadata columns.
      if(isset($this->config->metadata) && is_array($this->config->metadata)) {
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
      $this->dbObject->exec($this->config->pre_query);
    }
    $output = $this->dbObject->exec($build_command);

    if(isset($this->config->post_query) && !empty($this->config->post_query)){
      $this->dbObject->exec($this->config->post_query);
    }
    return $output;
  }

  /**
   * @param $columns
   * @return bool
   */
  public function create_table($columns){
    $build_command = null;
    foreach ($columns as $column_index => $column_name) {
      $c_name = implode('_', explode(' ', strtolower($column_name)));
      $create_table_fields[] = $c_name . ' TEXT';
    }
    $create_table_fields[] = 'record_processed TEXT DEFAULT N';
    $create_table_fields[] = 'image_processed TEXT DEFAULT N';
    $create_table_fields[] = 'updated_on DATETIME';
    $create_table_fields[] = 'created_on DATETIME';

    $create_table_fields_string = implode(', ', $create_table_fields);
    $create_table_statement = 'CREATE TABLE if not exists ' . $this->config->table_name . ' (' . $create_table_fields_string . ');';

    if(isset($this->config->debug) && $this->config->debug == 'yes'){
      echo $create_table_statement;
      return true;
    }
    return $this->dbObject->exec($create_table_statement);
  }

}