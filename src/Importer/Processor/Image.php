<?php
/**
 * Created by PhpStorm.
 * User: tarunjangra
 * Date: 27/09/2014
 * Time: 09:42
 */

namespace Importer\Processor;

class Image extends Base
{
  private $extension = '.jpg';

  public function __construct(&$config) {
    parent::__construct($config);
  }


  public function resize($input_name, $maxwidth, $maxheight, $square = FALSE,
                         $x1 = 0, $y1 = 0, $x2 = 0, $y2 = 0, $upscale = FALSE) {

    // Get the size information from the image
    $imgsizearray = getimagesize($input_name);
    if ($imgsizearray == FALSE) {
      return FALSE;
    }

    $width = $imgsizearray[0];
    $height = $imgsizearray[1];

    $accepted_formats = array(
      'image/jpeg' => 'jpeg',
      'image/pjpeg' => 'jpeg',
      'image/png' => 'png',
      'image/x-png' => 'png',
      'image/gif' => 'gif'
    );

    // make sure the function is available
    $load_function = "imagecreatefrom" . $accepted_formats[$imgsizearray['mime']];
    if (!is_callable($load_function)) {
      return FALSE;
    }

    // get the parameters for resizing the image
    $options = array(
      'maxwidth' => $maxwidth,
      'maxheight' => $maxheight,
      'square' => $square,
      'upscale' => $upscale,
      'x1' => $x1,
      'y1' => $y1,
      'x2' => $x2,
      'y2' => $y2,
    );
    $params = $this->getImageResizeParameters($width, $height, $options);
    if ($params == FALSE) {
      return FALSE;
    }

    // load original image
    $original_image = $load_function($input_name);
    if (!$original_image) {
      return FALSE;
    }

    // allocate the new image
    $new_image = imagecreatetruecolor($params['newwidth'], $params['newheight']);
    if (!$new_image) {
      return FALSE;
    }

    // color transparencies white (default is black)
    imagefilledrectangle(
      $new_image, 0, 0, $params['newwidth'], $params['newheight'],
      imagecolorallocate($new_image, 255, 255, 255)
    );

    $rtn_code = imagecopyresampled(	$new_image,
      $original_image,
      0,
      0,
      $params['xoffset'],
      $params['yoffset'],
      $params['newwidth'],
      $params['newheight'],
      $params['selectionwidth'],
      $params['selectionheight']);
    if (!$rtn_code) {
      return FALSE;
    }

    // grab a compressed jpeg version of the image
    ob_start();
    imagejpeg($new_image, NULL, 90);
    $jpeg = ob_get_clean();

    imagedestroy($new_image);
    imagedestroy($original_image);

    return $jpeg;
  }


  private function getImageResizeParameters($width, $height, $options) {

    $defaults = array(
      'maxwidth' => 100,
      'maxheight' => 100,

      'square' => FALSE,
      'upscale' => FALSE,

      'x1' => 0,
      'y1' => 0,
      'x2' => 0,
      'y2' => 0,
    );

    $options = array_merge($defaults, $options);

    extract($options);

    // crop image first?
    $crop = TRUE;
    if ($x1 == 0 && $y1 == 0 && $x2 == 0 && $y2 == 0) {
      $crop = FALSE;
    }

    // how large a section of the image has been selected
    if ($crop) {
      $selection_width = $x2 - $x1;
      $selection_height = $y2 - $y1;
    } else {
      // everything selected if no crop parameters
      $selection_width = $width;
      $selection_height = $height;
    }

    // determine cropping offsets
    if ($square) {
      // asking for a square image back

      // detect case where someone is passing crop parameters that are not for a square
      if ($crop == TRUE && $selection_width != $selection_height) {
        return FALSE;
      }

      // size of the new square image
      $new_width = $new_height = min($maxwidth, $maxheight);

      // find largest square that fits within the selected region
      $selection_width = $selection_height = min($selection_width, $selection_height);

      // set offsets for crop
      if ($crop) {
        $widthoffset = $x1;
        $heightoffset = $y1;
        $width = $x2 - $x1;
        $height = $width;
      } else {
        // place square region in the center
        $widthoffset = floor(($width - $selection_width) / 2);
        $heightoffset = floor(($height - $selection_height) / 2);
      }
    } else {
      // non-square new image
      $new_width = $maxwidth;
      $new_height = $maxheight;

      // maintain aspect ratio of original image/crop
      if (($selection_height / (float)$new_height) > ($selection_width / (float)$new_width)) {
        $new_width = floor($new_height * $selection_width / (float)$selection_height);
      } else {
        $new_height = floor($new_width * $selection_height / (float)$selection_width);
      }

      // by default, use entire image
      $widthoffset = 0;
      $heightoffset = 0;

      if ($crop) {
        $widthoffset = $x1;
        $heightoffset = $y1;
      }
    }

    if (!$upscale && ($selection_height < $new_height || $selection_width < $new_width)) {
      // we cannot upscale and selected area is too small so we decrease size of returned image
      if ($square) {
        $new_height = $selection_height;
        $new_width = $selection_width;
      } else {
        if ($selection_height < $new_height && $selection_width < $new_width) {
          $new_height = $selection_height;
          $new_width = $selection_width;
        }
      }
    }

    $params = array(
      'newwidth' => $new_width,
      'newheight' => $new_height,
      'selectionwidth' => $selection_width,
      'selectionheight' => $selection_height,
      'xoffset' => $widthoffset,
      'yoffset' => $heightoffset,
    );

    return $params;
  }
}