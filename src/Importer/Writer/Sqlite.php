<?php

namespace Importer\Writer;

class Sqlite extends Destination {

  public function __construct(&$config){
    parent::__construct($config);
  }


  public function update(&$records){
    $dbObject = new \SQLite3($this->config->workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.
      $this->config->workarea.DIRECTORY_SEPARATOR.$this->config->sqlite_db_file);

    $update_command = 'UPDATE ' . $this->config->table_name. ' SET ' ;
    $build_command = null;
    foreach ($records as $row_index => $row){
      foreach ($row as $rkey => $cell) {
        if (in_array($rkey, $this->config->skip_columns) || empty($rkey)){ continue; }
        $update_columns[] = $rkey."='".$dbObject->escapeString($cell)."'";
      }
      $update_columns[] = "record_processed='N'";

      $build_command .= $update_command. implode(", ", $update_columns). ' WHERE '.
        $this->config->keyfield."='".$row['username']."'; ";
      $update_columns= array();
    }
    return $dbObject->exec($build_command);
  }

}