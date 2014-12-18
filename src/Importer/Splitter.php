<?php
/**
 * Created by PhpStorm.
 * User: tarunjangra
 * Date: 18/12/14
 * Time: 18:38
 */

namespace Importer;


class Splitter {
  private $config = null;

  public function __construct(&$config){
    $this->config = $config;
  }

  public function split($size = 90000000){
    $done = false;
    $part = 0;
    $filename = basename($this->config->source_file,'.csv');
    if (($handle = fopen($this->config->source_file, "r")) !== FALSE) {
      $header = fgets($handle);
      while ($done == false) {
        $locA = ftell($handle); // gets the current location. START
        fseek($handle, $size, SEEK_CUR); // jump the length of $size from current position
        $tmp = fgets($handle); // read to the end of line. We want full lines
        $locB = ftell($handle); // gets the current location. END
        $span = ($locB - $locA);
        fseek($handle, $locA, SEEK_SET); // jump to the START of this chunk
        $chunk = fread($handle,$span); // read the chunk between START and END
        $destination = $this->config->workarea_root.date('Y/m/d').DIRECTORY_SEPARATOR.$this->config->workarea.DIRECTORY_SEPARATOR.$filename;
        file_put_contents($destination.'_'.$part.'.csv', $header.$chunk);
        $part++;
        if (strlen($chunk) < $size) $done = true;
      }
      fclose($handle);
    }
  }

}