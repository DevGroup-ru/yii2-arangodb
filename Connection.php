<?php

namespace devgroup\arangodb;

use \triagens\ArangoDb\ConnectionOptions;

class Connection extends \triagens\ArangoDb\Connection {
    public function json_encode_wrapper($data, $options = null)
    {
        if ($this->getOption(ConnectionOptions::OPTION_CHECK_UTF8_CONFORM) === true) {
            self::check_encoding($data);
        }

        
        $response = json_encode($data, $options | JSON_FORCE_OBJECT);
        
        return $response;
    }
}