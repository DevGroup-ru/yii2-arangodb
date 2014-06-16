<?php

namespace bethrezen\arangodb;

use yii\helpers\ArrayHelper;
use yii\base\Object;

// set up some aliases for less typing later
use triagens\ArangoDb\Connection as ArangoConnection;
use triagens\ArangoDb\ConnectionOptions as ArangoConnectionOptions;
use triagens\ArangoDb\DocumentHandler as ArangoDocumentHandler;

use triagens\ArangoDb\Document as ArangoDocument;
use triagens\ArangoDb\Exception as ArangoException;
use triagens\ArangoDb\ConnectException as ArangoConnectException;
use triagens\ArangoDb\ClientException as ArangoClientException;
use triagens\ArangoDb\ServerException as ArangoServerException;
use triagens\ArangoDb\UpdatePolicy as ArangoUpdatePolicy;
use triagens\ArangoDb\Statement as Statement;

class ArangoDbConnection extends Object {
    private $_connection = null;
    public $connectionOptions = [
        // server endpoint to connect to
        ArangoConnectionOptions::OPTION_ENDPOINT => 'tcp://127.0.0.1:8529',
        // authorization type to use (currently supported: 'Basic')
        ArangoConnectionOptions::OPTION_AUTH_TYPE => 'Basic',
        // user for basic authorization
        ArangoConnectionOptions::OPTION_AUTH_USER => 'root',
        // password for basic authorization
        ArangoConnectionOptions::OPTION_AUTH_PASSWD => '',
        // connection persistence on server. can use either 'Close' (one-time connections) or 'Keep-Alive' (re-used connections)
        ArangoConnectionOptions::OPTION_CONNECTION => 'Close',
        // connect timeout in seconds
        ArangoConnectionOptions::OPTION_TIMEOUT => 3,
        // whether or not to reconnect when a keep-alive connection has timed out on server
        ArangoConnectionOptions::OPTION_RECONNECT => true,
        // optionally create new collections when inserting documents
        ArangoConnectionOptions::OPTION_CREATE => true,
        // optionally create new collections when inserting documents
        ArangoConnectionOptions::OPTION_UPDATE_POLICY => ArangoUpdatePolicy::LAST,
    ];

    private $_collectionHandler = null;
    private $_documentHandler = null;

    public function __construct($config=[])
    {
        parent::__construct($config);
    }

    public function init()
    {
        parent::init();

        $this->_connection = new ArangoConnection($this->connectionOptions);
        $this->_collectionHandler = new \triagens\ArangoDb\CollectionHandler($this->_connection);
        $this->_documentHandler = new \triagens\ArangoDb\DocumentHandler($this->_connection);
    }

    public function getDocument($collection, $id) {
        return $this->documentHandler()->get($collection, $id);
    }

    public function documentHandler() {
        return $this->_documentHandler;
    }

    public function statement($options=[]) {
        return new Statement($this->_connection, $options);
    }

    public function collectionHandler() {
        return $this->_collectionHandler;
    }
}