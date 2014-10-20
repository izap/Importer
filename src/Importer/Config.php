<?php

namespace Importer;

class Config {

  private $_attributes;

  public function __construct($runtime_config = array()) {
    $workarea_root = __DIR__.DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.'..'.DIRECTORY_SEPARATOR.
      'workarea'.DIRECTORY_SEPARATOR;
    $global_config = json_decode(file_get_contents($workarea_root.'config.json'), true);
    $specific_config = json_decode(file_get_contents($workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.
      $runtime_config['workarea'].DIRECTORY_SEPARATOR.'config.json'), true);
    $this->_attributes = array_merge($global_config, $specific_config, $runtime_config);
    $this->_attributes['workarea_root'] = $workarea_root;
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

}