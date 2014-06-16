<?php

namespace bethrezen\arangodb;

use yii;
use yii\di\Instance;

use triagens\ArangoDb\Document;

use app;

class ArangoProvider extends yii\data\ActiveDataProvider
{
    public $arango = 'arango';

    public $collection;

    /**
     * @var array parameters for example
     */
    public $params = [];

    public function init()
    {
        parent::init();
        $this->arango = Instance::ensure($this->arango, app\components\ArangoDbConnection::className());
        if ($this->collection === null) {
            throw new InvalidConfigException('The "collection" property must be set.');
        }
    }

    /**
     * @inheritdoc
     */
    protected function prepareKeys($models)
    {
     
        return array_keys($models);

    }

    /**
     * @inheritdoc
     */
    protected function prepareModels()
    {
        // $sql = $this->sql;
        // $qb = $this->db->getQueryBuilder();
        // if (($sort = $this->getSort()) !== false) {
        //     $orderBy = $qb->buildOrderBy($sort->getOrders());
        //     if (!empty($orderBy)) {
        //         $orderBy = substr($orderBy, 9);
        //         if (preg_match('/\s+order\s+by\s+[\w\s,\.]+$/i', $sql)) {
        //             $sql .= ', ' . $orderBy;
        //         } else {
        //             $sql .= ' ORDER BY ' . $orderBy;
        //         }
        //     }
        // }

        // if (($pagination = $this->getPagination()) !== false) {
        //     $pagination->totalCount = $this->getTotalCount();
        //     $sql .= ' ' . $qb->buildLimit($pagination->getLimit(), $pagination->getOffset());
        // }

        // return $this->db->createCommand($sql, $this->params)->queryAll();
        $statement = $this->getBaseStatement();

        if (($pagination = $this->getPagination()) !== false) {
            $pagination->totalCount = $this->getTotalCount();
            $statement->setQuery($statement->getQuery() . "\n LIMIT " . $pagination->getOffset() . ", " . $pagination->getLimit());
        }

        
        $statement->setQuery($statement->getQuery()."\n RETURN a");
        $cursor = $statement->execute();
        $data = $cursor->getAll();
        $result = [];
        foreach ($data as $doc) {
            $item = $doc->getAll();
            foreach ($item as $k=>$v) {
                if (is_array($item[$k]) || is_object($item[$k])) {
                    $item[$k] = json_encode($v, true);
                }
            }
            $result[$item['_key']] = $item;
        }
        $pagination->totalCount = $cursor->getFullCount();
        
        return $result;
    }

    public function getTotalCount() {
        $statement = $this->getBaseStatement();
        $statement->setQuery($statement->getQuery(). "\n LIMIT 1 \n RETURN a");
        
        
        $cursor = $statement->execute();
        return $cursor->getFullCount();
    }

    private function getBaseStatement() {
        $query = "FOR a in @@collection\n";

        $filter = [];
        $bindings = ['@collection' => $this->collection];
        $counter = 0;
        foreach ($this->params as $k => $v) {
            $filter[] = " a.@filter_field_$counter == @filter_value_$counter ";
            $bindings["filter_field_$counter"] = $k;
            $bindings["filter_value_$counter"] = $v;
            $counter++;
        }
        if (count($filter)>0){
            $query .= "\nFILTER ".implode(" && ", $filter)."\n";
        }
        $statement = $this->arango->statement([
            'query' => $query,
            'count' => true,
            'bindVars' => $bindings,
            'fullCount' => true,
        ]);
        return $statement;
    }

    /**
     * @inheritdoc
     */
    protected function prepareTotalCount()
    {
        return 0;
    }
}