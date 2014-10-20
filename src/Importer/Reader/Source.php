<?php

namespace Importer\Reader;

abstract class Source {
  protected $config = null;

  /**
   * @param $config
   */
  public function __construct(&$config) {
    $this->config = $config;
  }

  abstract public function header();
  abstract public function records();

}