<?php

namespace devgroup\arangodb\console\controllers;

use devgroup\arangodb\Connection;
use devgroup\arangodb\Exception;
use devgroup\arangodb\Migration;
use devgroup\arangodb\Query;

use yii;
use yii\console\controllers\BaseMigrateController;
use yii\helpers\ArrayHelper;

use triagens\ArangoDb\ServerException;

class MigrateController extends BaseMigrateController
{
    /**
     * @var string the name of the collection for keeping applied migration information.
     */
    public $migrationCollection = 'migration';
    /**
     * @var string the directory storing the migration classes. This can be either
     * a path alias or a directory.
     */
    public $migrationPath = '@app/migrations/arangodb';
    /**
     * @inheritdoc
     */
    public $templateFile = '@devgroup/arangodb/views/migration.php';
    /**
     * @var Connection|string the DB connection object or the application
     * component ID of the DB connection.
     */
    public $db = 'arangodb';

    /**
     * @inheritdoc
     */
    public function options($actionId)
    {
        return array_merge(
            parent::options($actionId),
            ['migrationCollection', 'db'] // global for all actions
        );
    }

    /**
     * This method is invoked right before an action is to be executed (after all possible filters.)
     * It checks the existence of the [[migrationPath]].
     * @param yii\base\Action $action the action to be executed.
     * @throws Exception if db component isn't configured
     * @return boolean whether the action should continue to be executed.
     */
    public function beforeAction($action)
    {
        if (parent::beforeAction($action)) {
            if ($action->id !== 'create') {
                if (is_string($this->db)) {
                    $this->db = \Yii::$app->get($this->db);
                }
                if (!$this->db instanceof Connection) {
                    throw new Exception("The 'db' option must refer to the application component ID of a ArangoDB connection.");
                }
            }
            return true;
        } else {
            return false;
        }
    }

    /**
     * Creates a new migration instance.
     * @param string $class the migration class name
     * @return Migration the migration instance
     */
    protected function createMigration($class)
    {
        $file = $this->migrationPath . DIRECTORY_SEPARATOR . $class . '.php';
        require_once($file);

        return new $class(['db' => $this->db]);
    }

    /**
     * @inheritdoc
     */
    protected function getMigrationHistory($limit)
    {
        try {
            $history = $this->getHistory($limit);
        } catch (ServerException $ex) {
            if ($ex->getServerCode() == 1203) {
                $this->createMigrationHistoryCollection();
                $history = $this->getHistory($limit);
            } else {
                throw $ex;
            }
        }
        unset($history[self::BASE_MIGRATION]);

        return $history;
    }

    private function getHistory($limit)
    {
        $query = new Query;
        $rows = $query->select(['version' => 'version', 'apply_time' => 'apply_time'])
            ->from($this->migrationCollection)
            ->orderBy('version DESC')
            ->limit($limit)
            ->all($this->db);
        $history = ArrayHelper::map($rows, 'version', 'apply_time');
        unset($history[self::BASE_MIGRATION]);

        return $history;
    }

    protected function createMigrationHistoryCollection()
    {
        echo "Creating migration history collection \"$this->migrationCollection\"...";
        $this->db->getCollectionHandler()->create($this->migrationCollection);
        $this->db->getDocumentHandler()->save(
            $this->migrationCollection,
            [
                'version' => self::BASE_MIGRATION,
                'apply_time' => time(),
            ]
        );
        echo "done.\n";
    }

    /**
     * @inheritdoc
     */
    protected function addMigrationHistory($version)
    {
        $this->db->getDocumentHandler()->save(
            $this->migrationCollection,
            [
                'version' => $version,
                'apply_time' => time(),
            ]
        );
    }

    /**
     * @inheritdoc
     */
    protected function removeMigrationHistory($version)
    {
        $this->db->getCollectionHandler()->removeByExample(
            $this->migrationCollection,
            [
                'version' => $version,
            ]
        );
    }
}
