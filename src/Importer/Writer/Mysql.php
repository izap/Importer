<?php

namespace Importer\Writer;

class Mysql extends Destination {

  private $connection;
  public $last_error = '';
  private $i = 0;

  public function __construct(&$config) {
    parent::__construct($config);
    $this->connection = new \PDO("mysql:host={$this->config->db_host};dbname={$this->config->db_name};", $this->config->db_username, $this->config->db_password);
    $this->connection->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);
//     $this->connection->setAttribute(PDO::MYSQL_ATTR_INIT_COMMAND, "SET NAMES 'utf8'");
  }

  public function getData($query, $data, $cache = False) {
    $return = array();
    $statement = $this->connection->prepare($query);
    $statement->execute($data);
    while ($row = $statement->fetch(\PDO::FETCH_OBJ)) {
      $return[] = $row;
    }

    return $return;
  }

  private function __execute($statement, $data) {
    try {
      $query = $this->connection->prepare($statement);
      $saved = $query->execute($data);
    } catch (\PDOException $e) {
      echo $e->getMessage();
    }
    return $saved;
  }

  public function insert($rows) {
    echo "Starting Insert";
    if (sizeof($rows)) {
      foreach ($rows as $row) {
        echo "\n" . "--------------------------";
        echo "\n" . ++$this->i . " Username ---  " . $row['username'] . "   ";
        $saved = $this->__execute($row['mysql_query'], $row['mysql_data']);
        if ($saved) {
          echo "\n" . "--------------------------";
          echo "\n" . "Inserted username -  " . $row['username'];
        }
      }
    }
  }

  public function update($rows) {
    echo "Starting Update";
    if (sizeof($rows)) {
      foreach ($rows as $row) {
        echo "\n" . "--------------------------";
        echo "\n" . ++$this->i . " Username ---  " . $row['username'] . "   ";
        $saved = $this->__execute($row['mysql_query'], $row['mysql_data']);
        if ($saved) {
          echo "\n" . "--------------------------";
          echo "\n" . "Inserted username -  " . $row['username'];
        }
      }
    }
  }

  public function create_table($sql) {
    if (!$sql) {
      return False;
    }
    try{
    $this->connection->exec($sql);
    }  catch (\PDOException $e){
      echo $e->getMessage();  
    }
    echo "Done Creating";
  }

}
