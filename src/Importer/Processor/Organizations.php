<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 * Description of Teams
 *
 * @author ramandeep
 */

namespace Importer\Processor;

class Organizations extends Export {

  private $limit = 1;
  private $offset = 0;
  private $count;

  public function __construct(&$config) {
    parent::__construct($config);
  }

  public function all() {
    $es_entities = $this->getEntities();
    $this->count = $this->offset;
    while (count($es_entities['hits']['hits'])) {
      $return = array();
      foreach ($es_entities['hits']['hits'] as $key => $entity) {
        echo $this->count . " -----> " . $entity['_source']['guid'];
        echo "\n";
        $this->count++;
        $_data = array();
        $rss = array();
        $roles = array();
        $coach_array = array();
        $played_array = array();
        $work_array = array();
        $voice_array = array();

        $_data['sports_hometown'] = $this->getByGuid($entity['_source']['city_guid'], array('IzapGeoState', 'IzapGeoCity', 'IzapGeoCountry'));
        $_data['nationality'] = $this->getByGuid($entity['_source']['country_guid'], 'IzapGeoCountry');
        $_data['regionality'] = $this->getByGuid($entity['_source']['region_guid'], 'IzapGeoRegion');
        if ($entity['_source']['association_guid']) {
          $_data['association1'] = $this->getByGuid($entity['_source']['association_guid'], 'YsAssociation');
          $_data['association2'] = $this->getByGuid($entity['_source']['primary_association_guid'], 'YsAssociation');
        } else {
          $_data['association1'] = $this->getByGuid($entity['_source']['primary_association_guid'], 'YsAssociation');
        }

        $_data['skill'] = $this->getByGuid($entity['_source']['skill_guid'], 'YsSkill');
        $_data['skill_sport'] = $this->getByGuid($entity['_source']['skill_sport_guid'], 'YsSkillSports');
        $_data['sport'] = $this->getByGuid($entity['_source']['sport_guid'], 'YsSportsMetadata');
        $_data['home_town'] = $this->getByGuid($entity['_source']['city_guid'], array('IzapGeoState', 'IzapGeoCity', 'IzapGeoCountry'));
        $_data['school'] = $this->getByGuid($entity['_source']['highschool_guid'], 'YsSchool');
        if (!sizeof($_data['high_school'])) {
          $_data['school'] = $this->getByGuid($entity['_source']['college_guid'], 'YsSchool');
        }
        $_data['section'] = $this->getByGuid($entity['_source']['section_guid'], 'YsSection');
        $_data['website_url'] = (array) $entity['_source']['website_url'];
        $zip_code = '';
        $zip_code = (array) $entity['_source']['zip_code'];
        $_settings = $this->getSettings($entity['_source']['guid'], $entity['_source']['subtype']);
        if (count($_settings)) {
          foreach ($_settings as $s_key => $sett) { 
            if ($sett) {
              if ($s_key == 'fb_j') {
                foreach ($sett as $k => $s) {
                  if ($k == 'facebook_username') {
                    $_data['facebook_username'] = 'https://facebook.com/' . $s;
                  } elseif ($k == 'facebook_community_page') {
                    $_data['facebook_community_page'] = 'https://facebook.com/' . $s;
                  }
                }
              } elseif ($s_key == 'tw_j') {
                foreach ($sett as $k => $s) {
                  if ($k == 'twitter_username') {
                    $_data['twitter_username'] = 'https://twitter.com/' . $s;
                  }
                }
              } elseif ($s_key == 'yardbarker_url') {
                  $_data['yardbarker_url'] = $sett;

              } else {
                $_data['settings'][] = $sett;
              }
            }
          }
        }
        if ($entity['_source']['freebase_id']) {
          $_data['freebase_id'] = 'http://www.freebase.com' . $entity['_source']['freebase_id'];
        }
        if ($entity['_source']['full_name']) {
          $_data['full_name'] = str_replace("\"", "'", $entity['_source']['full_name']);
        }
        if ($entity['_source']['icon_Xlarge']) {
          $_data['image'] = 'https://d1ictct0z4sd3j.cloudfront.net' . $entity['_source']['icon_Xlarge'];
        } else {
          $_data['image'] = 'https://d1ictct0z4sd3j.cloudfront.net' . $entity['_source']['icon_large'];
        }
        if ($entity['_source']['helmet_icon_Xlarge']) {
          $_data['helmet_image'] = 'https://d1ictct0z4sd3j.cloudfront.net' . $entity['_source']['helmet_icon_Xlarge'];
        } elseif ($entity['_source']['helmet_icon_large']) {
          $_data['helmet_image'] = 'https://d1ictct0z4sd3j.cloudfront.net' . $entity['_source']['helmet_icon_large'];
        }
        if ($entity['_source']['website_url']) {
          $_data['website_url'] = (array) $entity['_source']['website_url'];
        }
        $rss_query = array(
            '_source' => array('rss_url'),
            'filter' => array(
                'term' => array(
                    'team_guid' => $entity['_source']['guid']
                )
            ), 'size' => 12
        );
        $rss_entity = $this->getByQuery($rss_query, array('YsRssPublisher'));
        $_data['voice'] = $this->getRelationData('voice_by', $entity['_source']['guid']);
        $_data['worked'] = $this->getRelationData('worked_by', $entity['_source']['guid']);
        $_data['played'] = $this->getRelationData('played_by', $entity['_source']['guid']);
        $_data['coached'] = $this->getRelationData('coached_by', $entity['_source']['guid']);
        foreach ($_data['voice'] as $voice_data) {
          $voice_array[] = $voice_data;
        }
        foreach ($_data['worked'] as $work_data) {
          $work_array[] = $work_data;
        }
        foreach ($_data['played'] as $played_data) {
          $played_array[] = $played_data;
        }
        foreach ($_data['coached'] as $coached_data) {
          $coach_array[] = $coached_data;
        }
//        print_r($entity);exit;
        $return[$key]['object'] = "Team";
        $return[$key]['subtype'] = $entity['_source']['subtype'];
        $return[$key]['organization_type'] = $entity['_source']['team_type'];
        $return[$key]['username'] = $entity['_source']['username'];
        $return[$key]['secondary_username'] = $entity['_source']['old_username'];
        $return[$key]['guid'] = $entity['_source']['guid'];
        $return[$key]['title'] = $entity['_source']['title'];
        $return[$key]['full_name'] = $_data['full_name'];
        $return[$key]['nickname1'] = $entity['_source']['nickname'];
        $return[$key]['tier'] = $entity['_source']['tier'];
        $return[$key]['gender'] = $entity['_source']['gender'];
        $return[$key]['status'] = $entity['_source']['status'];
        $return[$key]['sport'] = $_data['sport']['title'];
        $return[$key]['sport_username'] = $_data['sport']['username'];
        $return[$key]['sport_guid'] = $_data['sport']['guid'];
        $return[$key]['skill'] = $_data['skill']['title'];
        $return[$key]['skill_username'] = $_data['skill']['username'];
        $return[$key]['skill_guid'] = $_data['skill']['guid'];
        $return[$key]['skill_sport'] = $_data['skill_sport']['title'];
        $return[$key]['skill_sport_username'] = $_data['skill_sport']['username'];
        $return[$key]['skill_sport_guid'] = $_data['skill_sport']['guid'];
        $return[$key]['mascot'] = $entity['_source']['mascot'];
        $return[$key]['mascot_nickname'] = $entity['_source']['mascot_nickname'];
        $return[$key]['geography_primary'] = $_data['sports_hometown']['title'];
        $return[$key]['geography_primary_username'] = $_data['sports_hometown']['username'];
        $return[$key]['geography_primary_guid'] = $_data['sports_hometown']['guid'];
        $return[$key]['nationality'] = $_data['nationality']['title'];
        $return[$key]['nationality_username'] = $_data['nationality']['username'];
        $return[$key]['nationality_guid'] = $_data['nationality']['guid'];
        $return[$key]['regionality'] = $_data['regionality']['title'];
        $return[$key]['regionality_username'] = $_data['regionality']['username'];
        $return[$key]['regionality_guid'] = $_data['regionality']['guid'];
        $return[$key]['hometown'] = $_data['sports_hometown']['title'];
        $return[$key]['hometown_username'] = $_data['sports_hometown']['username'];
        $return[$key]['hometown_guid'] = $_data['sports_hometown']['guid'];
        $return[$key]['postal_code'] = $zip_code[0];
        $return[$key]['association1'] = $_data['association1']['title'];
        $return[$key]['association1_username'] = $_data['association1']['username'];
        $return[$key]['association1_guid'] = $_data['association1']['guid'];
        $return[$key]['association2'] = $_data['association2']['title'];
        $return[$key]['association2_username'] = $_data['association2']['username'];
        $return[$key]['association2_guid'] = $_data['association2']['guid'];
        $return[$key]['image1_url'] = $_data['image'];
        $return[$key]['helmet_image_url'] = $_data['helmet_image'];
        $return[$key]['youtube_topic_url'] = $entity['_source']['youtube_topic_url'];
        $return[$key]['freebase_id'] = $_data['freebase_id'];
        $return[$key]['rss1'] = $rss_entity['hits']['hits'][0]['_source']['rss_url'];
        $return[$key]['rss2'] = $rss_entity['hits']['hits'][1]['_source']['rss_url'];
        $return[$key]['rss3'] = $rss_entity['hits']['hits'][2]['_source']['rss_url'];
        $return[$key]['rss4'] = $rss_entity['hits']['hits'][3]['_source']['rss_url'];
        $return[$key]['rss5'] = $rss_entity['hits']['hits'][4]['_source']['rss_url'];
        $return[$key]['rss6'] = $rss_entity['hits']['hits'][5]['_source']['rss_url'];
        $return[$key]['rss7'] = $rss_entity['hits']['hits'][6]['_source']['rss_url'];
        $return[$key]['rss8'] = $rss_entity['hits']['hits'][7]['_source']['rss_url'];
        $return[$key]['rss9'] = $rss_entity['hits']['hits'][8]['_source']['rss_url'];
        $return[$key]['rss10'] = $rss_entity['hits']['hits'][9]['_source']['rss_url'];
        $return[$key]['rss11'] = $rss_entity['hits']['hits'][10]['_source']['rss_url'];
        $return[$key]['rss12'] = $rss_entity['hits']['hits'][11]['_source']['rss_url'];
        $return[$key]['yardbarker_rss'] = $entity['_source']['yardbarker_rss'];
        $return[$key]['website_url'] = $_data['website_url'][0];
        $return[$key]['wiki_url'] = $entity['_source']['wiki_url'];
        $return[$key]['facebook_username'] = $_data['facebook_username'];
        $return[$key]['facebook_community_page_url'] = $_data['facebook_community_page'];
        $return[$key]['twitter_username'] = $_data['twitter_username'];
        $return[$key]['youtube_official_url'] = (($entity['_source']['youtube_offical_url']) ? 'https://youtube.com' . $entity['_source']['youtube_offical_url'] : '');
        $return[$key]['yardbarker_url'] = $_data['yardbarker_url'];
        $return[$key]['object_id'] = $entity['_source']['code'];
        $return[$key]['clean_url'] = (($entity['_source']['clean_url']) ? "https://www.yoursports.com" . $entity['_source']['clean_url'] : '');
        $return[$key]['url'] = (($entity['_source']['url']) ? "https://www.yoursports.com" . $entity['_source']['url'] : '');
        $return[$key]['secondary_url'] = (($entity['_source']['old_username']) ? 'http://yoursports.com/' . $entity['_source']['old_username'] : '');
        $return[$key]['referencelink1_url'] = $_data['settings'][0];
        $return[$key]['referencelink2_url'] = $_data['settings'][1];
        $return[$key]['referencelink3_url'] = $_data['settings'][2];
        $return[$key]['referencelink4_url'] = $_data['settings'][3];
        $return[$key]['referencelink5_url'] = $_data['settings'][4];
        $return[$key]['referencelink6_url'] = $_data['settings'][5];
        $return[$key]['referencelink7_url'] = $_data['settings'][6];
        $return[$key]['referencelink8_url'] = $_data['settings'][7];
        $return[$key]['referencelink9_url'] = $_data['settings'][8];
        $return[$key]['referencelink10_url'] = $_data['settings'][9];
        $return[$key]['referencelink11_url'] = $_data['settings'][10];
        $return[$key]['referencelink12_url'] = $_data['settings'][11];

//        $return[$key]['oid'] = $entity['_source']['oid'];
//        $return[$key]['highschool'] = $_data['highschool']['title'];
//        $return[$key]['highschool_username'] = $_data['highschool']['username'];
//        $return[$key]['highschool_guid'] = $_data['highschool']['guid'];
//        $return[$key]['college1'] = $_data['college']['title'];
//        $return[$key]['college1_username'] = $_data['college']['username'];
//        $return[$key]['college1_guid'] = $_data['college']['guid'];

        $mysql = $this->getInsertQuery($return);
        $return_array[$key]['mysql_query'] = $mysql['query'];
        $return_array[$key]['username'] = $entity['_source']['username'];
        $return_array[$key]['mysql_data'] = $mysql['query_data'];
      }

      $data_string = Null;
      unset($data_string);
      $_data = Null;
      unset($_data);
      $es_entities = Null;
      unset($es_entities);
      $_settings = Null;
      unset($_settings);
      $played_array = Null;
      unset($played_array);
      $coach_array = Null;
      unset($coach_array);
      $work_array = Null;
      unset($work_array);
      $voice_array = Null;
      unset($voice_array);
      $rss_entity = Null;
      unset($rss_entity);

      $this->offset = $this->limit + $this->offset;
      echo 'offset =  ' . $this->offset;

      return $return_array;
    }
  }

  private function getSubtypes() {
    return array('YsTeam');
  }

  private function getFilePath($filename) {
    $file_path = elgg_get_data_path() . 'tmp_csv/' . $filename . '_' . date('d-m-Y') . '.csv';
    return $file_path;
  }

  private function getEntities() {
    $es_subtypes = $this->getSubtypes();
    $es_query = array('query' => array(
            'term' => array('username' => 'abilenechristianwildcatsbaseball')
        )
    );
//    $es_query = array('query' => array(
//        'match_all' => array()
//      ),
//      'sort' => array(
//        'guid' => 'desc'),
//      'size' => $this->limit, 'from' => $this->offset
//    );
    return $this->getByQuery($es_query, $es_subtypes);
  }

  private function getCounterFilePath($name) {
    $counter_file = elgg_get_data_path() . 'tmp_csv/' . $name . '_counter.csv';
    return $counter_file;
  }

  private function getInsertQuery($columns = array()) {

    if (!sizeof(columns)) {
      return False;
    }
    $data = array();
    $return = array();
    $insert_statement = "insert into organizations set ";
    foreach ($columns as $column) {
      foreach ($column as $column_name => $column_value) {
        if (isset($column_value) && !preg_match("/[^0-9]/", (string) $column_value)) {
          $name = ":" . ucfirst($column_name);
          $insert_statement .= "{$column_name} = {$name}, ";
          $data["{$name}"] = $column_value;
        } elseif (isset($column_value)) {
          $name = ":" . ucfirst($column_name);
          $insert_statement .= "{$column_name} = {$name}, ";
          $data["{$name}"] = "{$column_value}";
        }
      }
      $return['query'] = rtrim($insert_statement, ', ');
      $return['query_data'] = $data;
    }
    return $return;
  }

}
