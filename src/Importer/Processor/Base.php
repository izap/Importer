<?php
/**
 * Created by PhpStorm.
 * User: tarunjangra
 * Date: 21/12/14
 * Time: 20:33
 */

namespace Importer\Processor;


abstract class Base {
  protected $config = array();
  public function __construct(&$config){
    $this->config = $config;
  }
}