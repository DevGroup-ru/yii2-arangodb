<?php

namespace devgroup\arangodb\panels\arangodb;

use devgroup\arangodb\panels\arangodb\models\ArangoDb;
use Yii;
use yii\debug\Panel;
use yii\helpers\VarDumper;
use yii\log\Logger;

class ArangoDbPanel extends Panel
{
    /**
     * @var array db queries info extracted to array as models, to use with data provider.
     */
    private $_models;
    /**
     * @var array current database request timings
     */
    private $_timings;

    /**
     * Returns all profile logs of the current request for this panel. It includes categories such as:
     * 'yii\db\Command::query', 'yii\db\Command::execute'.
     * @return array
     */
    public function getProfileLogs()
    {
        $target = $this->module->logTarget;

        return $target->filterMessages(
            $target->messages,
            Logger::LEVEL_PROFILE,
            [
                'devgroup\arangodb\Query::query',
                'devgroup\arangodb\Query::execute',
            ]
        );
    }

    /**
     * Calculates given request profile timings.
     *
     * @return array timings [token, category, timestamp, traces, nesting level, elapsed time]
     */
    protected function calculateTimings()
    {
        if ($this->_timings === null) {
            $this->_timings = Yii::getLogger()->calculateTimings($this->data['arango-messages']);
        }

        return $this->_timings;
    }

    /**
     * Returns total query time.
     *
     * @param array $timings
     * @return integer total time
     */
    protected function getTotalQueryTime($timings)
    {
        $queryTime = 0;

        foreach ($timings as $timing) {
            $queryTime += $timing['duration'];
        }

        return $queryTime;
    }

    public function getName()
    {
        return 'Arango';
    }

    public function getSummary()
    {
        $timings = $this->calculateTimings();
        $queryCount = count($timings);
        $queryTime = number_format($this->getTotalQueryTime($timings) * 1000) . ' ms';

        return \Yii::$app->view->render(
            '@devgroup/arangodb/panels/arangodb/views/summary',
            [
                'timings' => $this->calculateTimings(),
                'queryCount' => $queryCount,
                'queryTime' => $queryTime,
                'panel' => $this,
            ]
        );
    }

    /**
     * @inheritdoc
     */
    public function getDetail()
    {
        $searchModel = new ArangoDb();
        $dataProvider = $searchModel->search(Yii::$app->request->getQueryParams(), $this->getModels());

        return Yii::$app->view->render('@devgroup/arangodb/panels/arangodb/views/detail', [
                'panel' => $this,
                'dataProvider' => $dataProvider,
                'searchModel' => $searchModel,
            ]);
    }

    /**
     * Returns an  array of models that represents logs of the current request.
     * Can be used with data providers such as \yii\data\ArrayDataProvider.
     * @return array models
     */
    protected function getModels()
    {
        if ($this->_models === null) {
            $this->_models = [];
            $timings = $this->calculateTimings();

            foreach ($timings as $seq => $dbTiming) {
                $this->_models[] = [
                    'query' => $dbTiming['info'],
                    'duration' => ($dbTiming['duration'] * 1000), // in milliseconds
                    'trace' => $dbTiming['trace'],
                    'timestamp' => ($dbTiming['timestamp'] * 1000), // in milliseconds
                    'seq' => $seq,
                ];
            }
        }

        return $this->_models;
    }

    public function save()
    {
        return ['arango-messages' => $this->getProfileLogs()];
    }
} 