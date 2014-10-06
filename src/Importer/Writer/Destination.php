<?php

namespace Importer\Writer;

abstract class Destination {
  protected $config = array();
  public function __construct(&$config){
    $this->config = $config;
  }
}