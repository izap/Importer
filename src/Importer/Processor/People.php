<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

namespace Importer\Processor;

class People extends Export {

  private $limit = 2;
  private $offset;
  private $count;

  public function __construct(&$config) {
    parent::__construct($config);
    $this->offset = $config->counter;
  }

  public function all() {
    $es_entities = $this->getEntities();
    $this->count = $this->offset;
    while (count($es_entities['hits']['hits'])) {
      $return = array();
      $return_array = array();
      foreach ($es_entities['hits']['hits'] as $key => $entity) {

        $relations_size = '';
        $blank_size = '';
        $roles = array();
        $coach_array = array();
        $played_array = array();
        $work_array = array();
        $voice_array = array();
        $_data = array();
//        echo $this->count . " -----> " . $entity['_source']['guid'];
        $this->count++;
        if ($entity['_source']['association_guid']) {
          $_data['association1'] = $this->getByGuid($entity['_source']['association_guid'], 'YsAssociation');
          $_data['association2'] = $this->getByGuid($entity['_source']['primary_association_guid'], 'YsAssociation');
        } else {
          $_data['association1'] = $this->getByGuid($entity['_source']['primary_association_guid'], 'YsAssociation');
        }
        $_data['highschool'] = $this->getByGuid($entity['_source']['highschool_guid'], 'YsSchool');
        $_data['highschool_team'] = $this->getByGuid($entity['_source']['highschool_team_guid'], 'YsTeam');
        if ($entity['_source']['old_username']) {
          $_data['secondary_url'] = "https://yoursports.com/" . $entity['_source']['old_username'];
        }
        $_data['college'] = $this->getByGuid($entity['_source']['college_guid'], 'YsSchool');
        $_data['secondary_college'] = $this->getByGuid($entity['_source']['secondary_college_guid'], 'YsSchool');
        $_data['college_team'] = $this->getByGuid($entity['_source']['college_team_guid'], 'YsTeam');
        if (sizeof($entity['_source']['birth'])) {
          $_data['date_of_birth_oid'] = $entity['_source']['birth']['date_oid'];
          $_data['birthplace'] = $this->getByGuid($entity['_source']['birth']['place_guid'], array('IzapGeoState', 'IzapGeoCity', 'IzapGeoCountry'));
        } else {
          $_data['birthplace'] = $this->getByGuid($entity['_source']['birthplace_guid'], array('IzapGeoState', 'IzapGeoCity', 'IzapGeoCountry'));
          $_data['date_of_birth_oid'] = $entity['_source']['date_of_birth_oid'];
        }
        if ($entity['_source']['draft_team_guid']) {
          $_data['draft_team'] = $this->getByGuid($entity['_source']['draft_team_guid'], array('YsTeam'));
        } elseif ($entity['_source']['draft_team_oid']) {
          $_data['draft_team'] = $this->getByOid($entity['_source']['draft_team_oid'], array('YsTeam'));
        }

        if (sizeof($entity['_source']['death'])) {
          $_data['date_of_death_oid'] = $entity['_source']['death']['date_oid'];
          $_data['life_status'] = $entity['_source']['death']['life_status'];
          $_data['life_period'] = $entity['_source']['death']['life_period'];
          $_data['deathplace'] = $this->getByGuid($entity['_source']['death']['place_guid'], array('IzapGeoState', 'IzapGeoCity', 'IzapGeoCountry'));
          $_data['death_cause'] = $entity['_source']['death']['cause'];
        } else {
          $_data['date_of_death_oid'] = $entity['_source']['date_of_death_oid'];
          $_data['life_status'] = $entity['_source']['life_status'];
          $_data['life_period'] = $entity['_source']['life_period'];
          $_data['deathplace'] = $this->getByGuid($entity['_source']['deathplace_guid'], array('IzapGeoState', 'IzapGeoCity', 'IzapGeoCountry'));
        }
        $_data['secondary_college_team'] = $this->getByGuid($entity['_source']['secondary_college_team_guid'], 'YsTeam');
        $_data['sports_hometown'] = $this->getByGuid($entity['_source']['city_guid'], array('IzapGeoState', 'IzapGeoCity', 'IzapGeoCountry'));
        $_data['nationality'] = $this->getByGuid($entity['_source']['nationality_guid'], 'IzapGeoCountry');
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
              'sports_person_guid' => $entity['_source']['guid']
            )
          ), 'size' => 1
        );
        $rss_entity = $this->getByQuery($rss_query, array('YsRssPublisher'));

        $_data['voice'] = $this->getRelationData('voice_of', $entity['_source']['guid']);
        $_data['worked'] = $this->getRelationData('worked_at', $entity['_source']['guid']);
        $_data['played'] = $this->getRelationData('played_at', $entity['_source']['guid'], array('REL_YsTeam', 'REL_YsAssociation'));
        $_data['coached'] = $this->getRelationData('coached_at', $entity['_source']['guid'], array('REL_YsTeam', 'REL_YsAssociation'));

        foreach ($_data['voice'] as $voice_data) {
          $voice_array[] = $voice_data;
        }
        foreach ($_data['worked'] as $work_data) {
          $work_array[] = $work_data;
        }

        foreach ($_data['played'] as $played_data) {

          if ($played_data['affiliation_guid'] == $entity['_source']['highschool_team_guid']) {
            $_data['highschool_team']['position'] = $played_data['pri_position'];
            $_data['highschool_team']['status'] = $played_data['status'];
          } elseif ($played_data['affiliation_guid'] == $entity['_source']['college_team_guid']) {
            $_data['college_team']['position'] = $played_data['pri_position'];
            $_data['college_team']['status'] = $played_data['status'];
          } elseif ($played_data['affiliation_guid'] == $entity['_source']['secondary_college_team_guid']) {
            $_data['secondary_college_team']['position'] = $played_data['pri_position'];
            $_data['secondary_college_team']['status'] = $played_data['status'];
          } else {
            $played_array[] = $played_data;
          }
        }
        foreach ($_data['coached'] as $coached_data) {
          $coach_array[] = $coached_data;
        }
        $_settings = $this->getSettings($entity['_source']['guid'], $entity['_source']['subtype']);
        if (count($_settings)) {
          foreach ($_settings as $s_key => $sett) {
            if ($sett) {
              if ($s_key == 'fb_j') {
                foreach ($sett as $k => $s) {
                  if ($s_key == 'facebook_username') {
                    $_data['facebook_username'] = $s;
                  } elseif ($s_key == 'facebook_community_page') {
                    $_data['facebook_community_page'] = $s;
                  }
                }
              } elseif ($s_key == 'tw_j') {
                foreach ($sett as $k => $s) {
                  if ($s_key == 'twitter_username') {
                    $_data['twitter_username'] = $s;
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
        $return[$key]['object'] = "Sports personality";
        $return[$key]['guid'] = $entity['_source']['guid'];
        $return[$key]['subtype'] = $entity['_source']['subtype'];
        $return[$key]['personality_type'] = $entity['_source']['personality_type'];
        $return[$key]['title'] = $entity['_source']['title'];
        $return[$key]['full_name'] = $_data['full_name'];
        $return[$key]['username'] = $entity['_source']['username'];
        $return[$key]['secondary_username'] = $entity['_source']['old_username'];
        $return[$key]['secondary_url'] = $_data['secondary_url'];
        $return[$key]['object_id'] = $entity['_source']['code'];
        $return[$key]['nickname1'] = $entity['_source']['nickname'];
        $return[$key]['nickname1_guid'] = $entity['_source']['nickname_guid'];
//        $return[$key]['oid'] = $entity['_source']['oid'];
        $return[$key]['association1'] = $_data['association1']['title'];
        $return[$key]['association1_username'] = $_data['association1']['username'];
        $return[$key]['association1_guid'] = $_data['association1']['guid'];
        $return[$key]['association2'] = $_data['association2']['title'];
        $return[$key]['association2_username'] = $_data['association2']['username'];
        $return[$key]['association2_guid'] = $_data['association2']['guid'];
        $return[$key]['highschool'] = $_data['highschool']['title'];
        $return[$key]['highschool_username'] = $_data['highschool']['username'];
        $return[$key]['highschool_guid'] = $_data['highschool']['guid'];
        $return[$key]['highschool_team'] = $_data['highschool_team']['title'];
        $return[$key]['highschool_team_username'] = $_data['highschool_team']['username'];
        $return[$key]['highschool_team_guid'] = $_data['highschool_team']['guid'];
        $return[$key]['highschool_team_status'] = $_data['highschool_team']['status'];
        $return[$key]['highschool_team_position'] = $_data['highschool_team']['position']['title'];
        $return[$key]['highschool_team_position_oid'] = $_data['highschool_team']['position']['oid'];
        $return[$key]['college1'] = $_data['college']['title'];
        $return[$key]['college1_username'] = $_data['college']['username'];
        $return[$key]['college1_guid'] = $_data['college']['guid'];
        $return[$key]['college1_team'] = $_data['college_team']['title'];
        $return[$key]['college1_team_username'] = $_data['college_team']['username'];
        $return[$key]['college1_team_guid'] = $_data['college_team']['guid'];
        $return[$key]['college1_team_status'] = $_data['college_team']['status'];
        $return[$key]['college1_team_position'] = $_data['college_team']['position']['title'];
        $return[$key]['college1_team_position_oid'] = $_data['college_team']['position']['oid'];
        $return[$key]['college2'] = $_data['secondary_college']['title'];
        $return[$key]['college2_username'] = $_data['secondary_college']['username'];
        $return[$key]['college2_guid'] = $_data['secondary_college']['guid'];
        $return[$key]['college2_team'] = $_data['secondary_college_team']['title'];
        $return[$key]['college2_team_username'] = $_data['secondary_college_team']['username'];
        $return[$key]['college2_team_guid'] = $_data['secondary_college_team']['guid'];
        $return[$key]['college2_team_status'] = $_data['secondary_college_team']['status'];
        $return[$key]['college2_team_position'] = $_data['secondary_college_team']['position']['title'];
        $return[$key]['college2_team_position_oid'] = $_data['secondary_college_team']['position']['oid'];
        $return[$key]['gender'] = $entity['_source']['gender'];
        $return[$key]['birthplace'] = $_data['birthplace']['title'];
        $return[$key]['birthplace_username'] = $_data['birthplace']['username'];
        $return[$key]['birthplace_guid'] = $_data['birthplace']['guid'];
        $return[$key]['deathplace'] = $_data['deathplace']['title'];
        $return[$key]['deathplace_username'] = $_data['deathplace']['username'];
        $return[$key]['deathplace_guid'] = $_data['deathplace']['guid'];
        $return[$key]['hometown'] = $_data['sports_hometown']['title'];
        $return[$key]['hometown_username'] = $_data['sports_hometown']['username'];
        $return[$key]['hometown_guid'] = $_data['sports_hometown']['guid'];
        $return[$key]['wiki_url'] = $entity['_source']['wiki_url'];
        $return[$key]['website_url1'] = $_data['website_url'][0];
        $return[$key]['website_url2'] = $_data['website_url'][1];
        $return[$key]['website_url3'] = $_data['website_url'][2];
        $return[$key]['website_url4'] = $_data['website_url'][3];
        $return[$key]['website_url5'] = $_data['website_url'][4];
        $return[$key]['website_url6'] = $_data['website_url'][5];
        $return[$key]['website_url7'] = $_data['website_url'][6];
        $return[$key]['website_url8'] = $_data['website_url'][7];
        $return[$key]['website_url9'] = $_data['website_url'][8];
        $return[$key]['website_url10'] = $_data['website_url'][9];
        $return[$key]['draft_year'] = $entity['_source']['draft_year'];
        $return[$key]['draft_year_oid'] = $entity['_source']['draft_year_oid'];
        $return[$key]['draft_round'] = $entity['_source']['draft_round'];
        $return[$key]['draft_round_oid'] = $entity['_source']['draft_round_oid'];
        $return[$key]['draft_position'] = $entity['_source']['draft_position'];
        $return[$key]['draft_position_oid'] = $entity['_source']['draft_position_oid'];
        $return[$key]['draft_order'] = $entity['_source']['draft_order'];
        $return[$key]['draft_order_data'] = $entity['_source']['draft_order_data'];
        $return[$key]['draft_team'] = $_data['draft_team']['title'];
        $return[$key]['draft_team_username'] = $_data['draft_team']['username'];
        $return[$key]['draft_team_guid'] = $_data['draft_team']['guid'];
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
        $return[$key]['image1_url'] = $_data['image'];
        $return[$key]['helmet_image_url'] = $_data['helmet_image'];
        $return[$key]['video1_url'] = $entity['_source']['moments_video_url_original'];
        $return[$key]['date_of_debut_oid'] = $entity['_source']['date_of_debut_oid'];
        $return[$key]['date_of_birth_oid'] = $entity['_source']['date_of_birth_oid'];
        $return[$key]['date_of_death_oid'] = $entity['_source']['date_of_death_oid'];
        $return[$key]['height_oid'] = $entity['_source']['height_oid'];
        $return[$key]['weight_oid'] = $entity['_source']['weight_oid'];
        $return[$key]['throws'] = $entity['_source']['throws'];
        $return[$key]['bats'] = $entity['_source']['bats'];
        $return[$key]['shoots'] = $entity['_source']['shoots'];
        $return[$key]['date_of_final_game_oid'] = $entity['_source']['date_of_final_game_oid'];
        $return[$key]['facebook_username'] = $_data['facebook_username'];
        $return[$key]['facebook_community_page_url'] = $_data['facebook_community_page'];
        $return[$key]['twitter_username'] = $_data['twitter_username'];
        $return[$key]['youtube_topic_url'] = $entity['_source']['youtube_topic_url'];
        $return[$key]['yardbarker_rss'] = $entity['_source']['yardbarker_rss'];
        $return[$key]['yardbarker_url'] =  $_data['yardbarker_url'];
        $return[$key]['nationality'] = $_data['nationality']['title'];
        $return[$key]['nationality_username'] = $_data['nationality']['username'];
        $return[$key]['nationality_guid'] = $_data['nationality']['guid'];
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
        $return[$key]['clean_url'] = "https://yoursports.com/" . $entity['_source']['clean_url'];
        $return[$key]['url'] = "https://yoursports.com/" . $entity['_source']['url'];
        if ($entity['_source']['youtube_official_url']) {
          $return[$key]['youtube_official_url'] = "https://yoursports.com/" . $entity['_source']['youtube_official_url'];
        }
        $roles = array_merge($played_array, $coach_array, $work_array, $voice_array);
        print_r($roles);
        if (sizeof($roles)) {
          $counter = 1;
          foreach ($roles as $r => $role) {
            foreach ($role as $r_key => $r_val) {
              $return[$key]["role" . $counter . "_" . $r_key] = $r_val;
            }
            $counter++;
          }
        }
        $mysql = $this->getInsertQuery($return[$key]);
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
      $this->offset = $this->offset + $this->limit;
//      file_put_contents($counter_file, $this->offset);
      $es_entities = $this->getEntities();
      return $return_array;
    }
  }

  private function getSubtypes() {
    return array('YsAthlete', 'YsCoach', 'YsSportsPerson', 'YsMediaPerson', 'YsTeamExecutive');
  }

  private function getFilePath($filename) {
    $file_path = elgg_get_data_path() . 'tmp_csv/' . $filename . '_' . date('d-m-Y') . '.csv';
    return $file_path;
  }

  private function getEntities() {
    $es_subtypes = $this->getSubtypes();
    $es_query = array('query' => array(
        'term' => array(
          'guid' => 3266719
        )
      ),
      'size' => $this->limit, 'from' => $this->offset
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
    $insert_statement = "insert into people set ";
    foreach ($columns as $column_name => $column_value) {
      if (isset($column_value) && !preg_match("/[^0-9]/", (string) $column_value)) {
        $name = ":" . ucfirst($column_name);
        $insert_statement .= "{$column_name} = {$name}, ";
        $data["{$name}"] = $column_value;
      } elseif (isset($column_value)) {
        $name = ":" . ucfirst($column_name);
        $insert_statement .= "{$column_name} = {$name}, ";
        $data["{$name}"] = "{$column_value}";
      }
      $return['query'] = rtrim($insert_statement, ', ');
      $return['query_data'] = $data;
    }
    return $return;
  }

}
