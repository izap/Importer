<?php

namespace Importer\Reader;

class Elasticsearch extends Source {

  private $request_url;

  public function __construct(&$config) {
    parent::__construct($config);
  }

  private function setRequestUrl($subtypes) {
    $this->request_url = "http://" . $this->config->es_host . ':' . $this->config->es_port . '/' .
      $this->config->es_index . '/' . $subtypes . '/_search?pretty=true';
  }

  public function getRequestUrl() {
    return $this->request_url;
  }

  public function records($params = array()) {
    if (isset($this->config->table_name)) {
      $process = '\\Importer\\Processor\\' . ucfirst($this->config->table_name);
      $process_object = new $process($this->config);
      $rows = $process_object->all();
      return $rows;
    }
    return False;
  }

  public function get_data($query, $subtypes = array()) {
    $subtypes = implode(',', $subtypes);
    $this->setRequestUrl($subtypes);
    global $CONFIG;
    $default = array('index' => $this->config->es_index);
    $url = $this->getRequestUrl();
    $resp = $this->request($url, $query);
    return $resp;
  }

  /**
   * Makes an HTTP GET request to the specified $url with an optional array or string of $vars
   *
   * Returns a CurlResponse object if the request was successful, false otherwise
   *
   * @param string $url
   * @param array|string $vars 
   * @return CurlResponse
   * */
  private function request($url, $query, $method = 'GET') {
    $query = json_encode($query);
    // Get cURL resource
    $curl = curl_init();
    curl_setopt_array($curl, array(
      CURLOPT_URL => $url,
      CURLOPT_PORT => $this->config->es_port,
      CURLOPT_TIMEOUT => 200,
      CURLOPT_HTTPHEADER => array(
        'Content-Type: application/json',
        'Content-Length: ' . strlen($query)),
      CURLOPT_RETURNTRANSFER => 1,
      CURLOPT_FORBID_REUSE => 0,
      CURLOPT_CUSTOMREQUEST => $method,
      CURLOPT_POSTFIELDS => $query
    ));
    $response = curl_exec($curl);
    curl_close($curl);
    if ($response) {
      return json_decode($response, True);
    } else {
      $error = curl_errno($curl) . ' - ' . curl_error($curl);
      print_r($error);
      exit;
    }
  }

}