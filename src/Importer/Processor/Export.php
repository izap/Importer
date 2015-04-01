<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace Importer\Processor;

class Export extends Base {

  protected $config = array();
  private $reader_object;
  public static $CACHED = array();

  public function __construct(&$config) {
    parent::__construct($config);
    $reader = '\\Importer\\Reader\\' . ucfirst($this->config->reader['driver']);
    $this->reader_object = new $reader($this->config);
  }

  protected function getByQuery($query, $subtypes = array()) {
    if (!$query) {
      return False;
    }
    $rows = $this->reader_object->get_data($query, (array) $subtypes);
    return $rows;
  }

  protected function getByGuid($guid, $subtype = array()) {
    if (!$guid) {
      return False;
    }
    if (self::$CACHED[$guid]) {
      return self::$CACHED[$guid];
    }

    $query = array(
      '_source' => array('guid', 'title', 'name', 'subtype', 'username', 'old_username', 'oid', 'nickname', 'code', 'full_name', 'mascot'),
      'filter' => array(
        'term' => array(
          'guid' => $guid
        )
      ), 'size' => 1
    );
    $data = $this->getByQuery($query, (array) $subtype);
    if ($data['hits']['hits'][0]['_source']['guid']) {
      self::$CACHED[$guid] = $data['hits']['hits'][0]['_source'];
      return self::$CACHED[$guid];
    }
    return False;
  }

  protected function getByOid($oid, $subtype = array()) {
    if (!$oid) {
      return False;
    }
    if (self::$CACHED[$oid]) {
      return self::$CACHED[$oid];
    }

    $query = array(
      '_source' => array('guid', 'title', 'name', 'subtype', 'username', 'old_username', 'oid', 'nickname', 'code', 'full_name', 'mascot'),
      'filter' => array(
        'term' => array(
          'oid' => $oid
        )
      ), 'size' => 1
    );
    $data = $this->getByQuery($query, (array) $subtype);
    if ($data['hits']['hits'][0]['_source']['oid']) {
      self::$CACHED[$oid] = $data['hits']['hits'][0]['_source'];
      return self::$CACHED[$oid];
    }
    return False;
  }

  protected function getByUsername($username, $subtype = array()) {
    if (!$username) {
      return False;
    }
    if (self::$CACHED[$username]) {
      return self::$CACHED[$username];
    }

    $query = array(
      '_source' => array('guid', 'title', 'name', 'subtype', 'username', 'old_username', 'oid', 'nickname', 'code', 'full_name', 'mascot'),
      'filter' => array(
        'term' => array(
          'username' => $username
        )
      ), 'size' => 1
    );
    $data = $this->getByQuery($query, (array) $subtype);
    if ($data['hits']['hits'][0]['_source']['username']) {
      self::$CACHED[$username] = $data['hits']['hits'][0]['_source'];
      return self::$CACHED[$username];
    }
    return False;
  }

  protected function getRelationData($rel, $guid, $subtype = array()) {
    if (!$guid) {
      return False;
    }
    $data = array();
    $relation_query = array(
      'query' => array(
        'bool' => array(
          'must' => array(
            array('term' => array('guid_one' => $guid)),
            array('term' => array('rel' => $rel))
          )
        )
      )
    );
    $relation_entity = $this->getByQuery($relation_query, $subtype);

    foreach ($relation_entity['hits']['hits'] as $key => $relation) {
      if (sizeof($subtype)) {
        $entity_subtypes = $this->getSubtypeFromRelationType($subtype);
      }
      $aff_obj = $this->getByGuid($relation['_source']['guid_two'], $entity_subtypes);
      if ($aff_obj) {
        $positions = $this->_getPositions($relation);
        $data[$aff_obj['username']] = array('affiliation_username' => $aff_obj['username'],
          'affiliation' => $aff_obj['title'],
          'affiliation_guid' => $aff_obj['guid'],
          'subtype' => $aff_obj['subtype'],
          'position' => $positions['primary']['title'],
          'position_oid' => $positions['primary']['oid'],
//          'position1' => $positions['primary']['title'],
//          'position1_oid' => $positions['primary']['oid'],
          'position2' => $positions['secondary']['title'],
          'position2_oid' => $positions['secondary']['oid'],
          'status' => $relation['_source']['relation_value']['status'],
//          'team_type' => $relation['_source']['relation_value']['team_type'],
//          'school_level' => $relation['_source']['relation_value']['school_level'],
          'is_staff' => $relation['_source']['relation_value']['is_team_staff'],
          'is_executive' => $relation['_source']['relation_value']['is_team_executive'],
          'is_owner' => $relation['_source']['relation_value']['is_team_owner'],
          'season' => $relation['_source']['season'],
          'relation_type' => $rel
        );
      }
    }
    return $data;
  }

  private function _getPositions($relation = array()) {
    $position = array();
    if ($relation['_source']['position_priority'] == 1) {
      $primary_position = $this->getByGuid($relation['_source']['position_guid'], 'YsPosition');
      $position['primary']['title'] = $primary_position['title'];
      $position['primary']['nickname'] = $primary_position['nickname'];
      $position['primary']['oid'] = $primary_position['oid'];
    }
    if ($relation['_source']['position_priority'] > 1) {
      $secondary_position = $this->getByGuid($relation['_source']['position_guid'], 'YsPosition');
      $position['secondary']['title'] = $secondary_position['title'];
      $position['secondary']['nickname'] = $secondary_position['nickname'];
      $position['secondary']['oid'] = $secondary_position['oid'];
    }
    return $position;
  }

  protected function getSettings($entity_guid, $subtype) {
    $_data = Null;
    unset($_data);
    if ($entity_guid) {
      $settings_query = array(
        'query' => array(
          'term' => array(
            '_id' => $entity_guid
          )
        ), 'size' => 1
      );
      $settings = $this->getByQuery($settings_query, $subtype . 'Settings');
    }
    if (sizeof($settings['hits']['hits'])) {
      foreach ($settings['hits']['hits'][0]['_source'] as $key => $setting) {
        $_data[$key] = $setting;
      }
    }
    return $_data;
  }

  private function getSubtypeFromRelationType($relation_type) {
    if (!sizeof($relation_type)) {
      return False;
    }
    $subtype = array();
    foreach ($relation_type as $type) {
      $stype = explode('_', $type);
      $subtype[] = $stype[1];
    }
    return $subtype;
  }

}
