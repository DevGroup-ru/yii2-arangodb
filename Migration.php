<?php

namespace devgroup\arangodb;

use yii\base\Component;
use yii\db\MigrationInterface;
use yii\di\Instance;
use yii\helpers\ArrayHelper;

abstract class Migration extends Component implements MigrationInterface
{
    /**
     * @var Connection|string the DB connection object or the application component ID of the DB connection
     * that this migration should work with.
     */
    public $db = 'arangodb';

    /**
     * Initializes the migration.
     * This method will set [[db]] to be the 'db' application component, if it is null.
     */
    public function init()
    {
        parent::init();
        $this->db = Instance::ensure($this->db, Connection::className());
    }

    public function execute($aql, $bindValues = [], $params = [])
    {
        echo "    > execute AQL: $aql ...";
        $time = microtime(true);
        $options = [
            'query' => $aql,
            'bindValues' => $bindValues,
        ];
        $options = ArrayHelper::merge($params, $options);
        $statement = $this->db->getStatement($options);
        $statement->execute();
        echo " done (time: " . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }

    public function insert($collection, $columns, $params = [])
    {
        echo "    > insert into $collection ...";
        $time = microtime(true);
        (new Query())->insert($collection, $columns, $params);
        echo " done (time: " . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }

    public function update($collection, $columns, $condition = '', $params = [])
    {
        echo "    > update $collection ...";
        $time = microtime(true);
        (new Query())->update($collection, $columns, $condition, $params);
        echo " done (time: " . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }

    public function delete($collection, $condition = '', $params = [])
    {
        echo "    > delete from $collection ...";
        $time = microtime(true);
        (new Query())->remove($collection, $condition, $params);
        echo " done (time: " . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }

    public function createCollection($collection, $options = [])
    {
        echo "    > create collection $collection ...";
        $time = microtime(true);
        $this->db->getCollectionHandler()->create($collection, $options);
        echo " done (time: " . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }

    public function dropCollection($collection)
    {
        echo "    > drop collection $collection ...";
        $time = microtime(true);
        $this->db->getCollectionHandler()->drop($collection);
        echo " done (time: " . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }

    public function truncateCollection($collection)
    {
        echo "    > truncate collection $collection ...";
        $time = microtime(true);
        $this->db->getCollectionHandler()->truncate($collection);
        echo " done (time: " . sprintf('%.3f', microtime(true) - $time) . "s)\n";
    }
}
