<?php

namespace devgroup\arangodb;

use Yii;
use triagens\ArangoDb\Document;
use triagens\ArangoDb\Statement;
use yii\base\Component;
use yii\base\InvalidParamException;
use yii\base\NotSupportedException;
use yii\db\QueryInterface;
use yii\helpers\ArrayHelper;
use yii\helpers\Json;

class Query extends Component implements QueryInterface
{
    const PARAM_PREFIX = 'qp';

    public $separator = " ";

    protected $conditionBuilders = [
        'NOT' => 'buildNotCondition',
        'AND' => 'buildAndCondition',
        'OR' => 'buildAndCondition',
        'IN' => 'buildInCondition',
        'LIKE' => 'buildLikeCondition',
        'BETWEEN' => 'buildBetweenCondition',
    ];

    protected $conditionMap = [
        'NOT' => '!',
        'AND' => '&&',
        'OR' => '||',
        'IN' => 'in',
        'LIKE' => 'LIKE',
    ];

    public $select = [];

    public $from;

    public $where;

    public $limit;

    public $offset;

    public $orderBy;

    public $indexBy;

    public $params = [];

    public $options = [];

    /**
     * @param array $options
     * @param null|Connection $db
     * @return null|Statement
     */
    private function getStatement($options = [], $db = null)
    {
        if ($db === null) {
            $db = Yii::$app->get('arangodb');
        }

        return $db->getStatement($options);
    }

    /**
     * @param $aql
     * @param $params
     * @return array [$aql, $params]
     */
    private static function prepareBindVars($aql, $params)
    {
        $search = [];
        $replace = [];

        foreach ($params as $key => $value) {
            if (is_array($value)) {
                $search[] = "@$key";
                $replace[] = json_encode($value);
                unset($params[$key]);
            }
        }

        if (count($search)) {
            $aql = str_replace($search, $replace, $aql);
        }

        return [$aql, $params];
    }

    /**
     * @param null|Connection $db
     * @param array $options
     * @return null|Statement
     */
    public function createCommand($db = null, $options = [])
    {
        list ($aql, $params) = $this->buildQuery($this);

        $options = ArrayHelper::merge(
            $options,
            [
                'query' => $aql,
                'bindVars' => $params,
            ]
        );

        return $this->getStatement($options, $db);
    }

    /**
     * @param $aql
     * @param array $bindValues
     * @param array $params
     * @return array
     * @throws \Exception
     */
    public function execute($aql, $bindValues = [], $params = [])
    {
        list ($aql, $bindValues) = self::prepareBindVars($aql, $bindValues);

        $options = [
            'query' => $aql,
            'bindVars' => $bindValues,
        ];

        $options = ArrayHelper::merge($params, $options);
        $statement = $this->getStatement($options);
        $token = $this->getRawAql($statement);
        Yii::info($token, 'devgroup\arangodb\Query::query');
        try {
            Yii::beginProfile($token, 'devgroup\arangodb\Query::query');
            $cursor = $statement->execute();
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
        } catch (\Exception $ex) {
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
            throw new \Exception($ex->getMessage(), (int) $ex->getCode(), $ex);
        }
        return $this->prepareResult($cursor->getAll());
    }

    /**
     * @param $fields
     * @return $this
     */
    public function select($fields)
    {
        $this->select = $fields;

        return $this;
    }

    /**
     * @param $collection
     * @return $this
     */
    public function from($collection)
    {
        $this->from = $collection;

        return $this;
    }

    /**
     * @param $collection
     * @return string
     */
    protected function buildFrom($collection)
    {
        $collection = trim($collection);
        return $collection ? "FOR $collection IN $collection" : '';
    }

    /**
     * @param $name
     * @return string
     */
    public function quoteCollectionName($name)
    {
        if (strpos($name, '(') !== false || strpos($name, '{{') !== false) {
            return $name;
        }
        if (strpos($name, '.') === false) {
            return $name;
        }
        $parts = explode('.', $name);
        foreach ($parts as $i => $part) {
            $parts[$i] = $part;
        }

        return implode('.', $parts);

    }

    /**
     * @param $name
     * @return string
     */
    public function quoteColumnName($name)
    {
        if (strpos($name, '(') !== false || strpos($name, '[[') !== false || strpos($name, '{{') !== false) {
            return $name;
        }
        if (($pos = strrpos($name, '.')) !== false) {
            $prefix = substr($name, 0, $pos);
            $prefix = $this->quoteCollectionName($prefix) . '.';
            $name = substr($name, $pos + 1);
        } else {
            $prefix = $this->quoteCollectionName($this->from) . '.';
        }

        return $prefix . $name;
    }

    /**
     * @param $condition
     * @param $params
     * @return string
     */
    protected function buildWhere($condition, &$params)
    {
        $where = $this->buildCondition($condition, $params);

        return $where === '' ? '' : 'FILTER ' . $where;
    }

    /**
     * @param $condition
     * @param $params
     * @return string
     */
    protected function buildCondition($condition, &$params)
    {
        if (!is_array($condition)) {
            return (string) $condition;
        } elseif (empty($condition)) {
            return '';
        }

        if (isset($condition[0])) { // operator format: operator, operand 1, operand 2, ...
            $operator = strtoupper($condition[0]);
            if (isset($this->conditionBuilders[$operator])) {
                $method = $this->conditionBuilders[$operator];
                array_shift($condition);
                return $this->$method($operator, $condition, $params);
            } else {
                throw new InvalidParamException('Found unknown operator in query: ' . $operator);
            }
        } else { // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
            return $this->buildHashCondition($condition, $params);
        }
    }

    /**
     * @param $condition
     * @param $params
     * @return string
     * @throws Exception
     */
    protected function buildHashCondition($condition, &$params)
    {
        $parts = [];
        foreach ($condition as $column => $value) {
            if (is_array($value) || $value instanceof Query) {
                // IN condition
                $parts[] = $this->buildInCondition('IN', [$column, $value], $params);
            } else {
                if (strpos($column, '(') === false) {
                    $column = $this->quoteColumnName($column);
                }
                if ($value === null) {
                    $parts[] = "$column == null";
                } else {
                    $phName = self::PARAM_PREFIX . count($params);
                    $parts[] = "$column==@$phName";
                    $params[$phName] = $value;
                }
            }
        }
        return count($parts) === 1 ? $parts[0] : '(' . implode(') && (', $parts) . ')';
    }

    /**
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     */
    protected function buildAndCondition($operator, $operands, &$params)
    {
        $parts = [];
        foreach ($operands as $operand) {
            if (is_array($operand)) {
                $operand = $this->buildCondition($operand, $params);
            }
            if ($operand !== '') {
                $parts[] = $operand;
            }
        }
        if (!empty($parts)) {
            return '(' . implode(") {$this->conditionMap[$operator]} (", $parts) . ')';
        } else {
            return '';
        }
    }

    /**
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     */
    protected function buildNotCondition($operator, $operands, &$params)
    {
        if (count($operands) != 1) {
            throw new InvalidParamException("Operator '$operator' requires exactly one operand.");
        }

        $operand = reset($operands);
        if (is_array($operand)) {
            $operand = $this->buildCondition($operand, $params);
        }
        if ($operand === '') {
            return '';
        }

        return "{$this->conditionMap[$operator]} ($operand)";
    }

    /**
     * @param $operator
     * @param $operands
     * @param $params
     * @return string
     * @throws Exception
     */
    protected function buildInCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1])) {
            throw new Exception("Operator '$operator' requires two operands.");
        }

        list($column, $values) = $operands;

        if ($values === [] || $column === []) {
            return $operator === 'IN' ? '0==1' : '';
        }

        if ($values instanceof Query) {
            // sub-query
            list($sql, $params) = $this->buildQuery($values, $params);
            $column = (array)$column;
            if (is_array($column)) {
                foreach ($column as $i => $col) {
                    if (strpos($col, '(') === false) {
                        $column[$i] = $this->quoteColumnName($col);
                    }
                }
                return '(' . implode(', ', $column) . ") {$this->conditionMap[$operator]} ($sql)";
            } else {
                if (strpos($column, '(') === false) {
                    $column = $this->quoteColumnName($column);
                }
                return "$column {$this->conditionMap[$operator]} ($sql)";
            }
        }

        $values = (array) $values;

        if (count($column) > 1) {
            return $this->buildCompositeInCondition($operator, $column, $values, $params);
        }

        if (is_array($column)) {
            $column = reset($column);
        }
        foreach ($values as $i => $value) {
            if (is_array($value)) {
                $value = isset($value[$column]) ? $value[$column] : null;
            }
            if ($value === null) {
                $values[$i] = 'null';
            } else {
                $phName = self::PARAM_PREFIX . count($params);
                $params[$phName] = $value;
                $values[$i] = "@$phName";
            }
        }
        if (strpos($column, '(') === false) {
            $column = $this->quoteColumnName($column);
        }

        if (count($values) > 1) {
            return "$column {$this->conditionMap[$operator]} [" . implode(', ', $values) . ']';
        } else {
            $operator = $operator === 'IN' ? '==' : '!=';
            return $column . $operator . reset($values);
        }
    }

    /**
     * @param $operator
     * @param $columns
     * @param $values
     * @param $params
     * @return string
     */
    protected function buildCompositeInCondition($operator, $columns, $values, &$params)
    {
        $vss = [];
        foreach ($values as $value) {
            $vs = [];
            foreach ($columns as $column) {
                if (isset($value[$column])) {
                    $phName = self::PARAM_PREFIX . count($params);
                    $params[$phName] = $value[$column];
                    $vs[] = "@$phName";
                } else {
                    $vs[] = 'null';
                }
            }
            $vss[] = '(' . implode(', ', $vs) . ')';
        }
        foreach ($columns as $i => $column) {
            if (strpos($column, '(') === false) {
                $columns[$i] = $this->quoteColumnName($column);
            }
        }

        return '(' . implode(', ', $columns) . ") {$this->conditionMap[$operator]} [" . implode(', ', $vss) . ']';
    }

    /**
     * Creates an SQL expressions with the `BETWEEN` operator.
     * @param string $operator the operator to use
     * @param array $operands the first operand is the column name. The second and third operands
     * describe the interval that column value should be in.
     * @param array $params the binding parameters to be populated
     * @return string the generated AQL expression
     * @throws InvalidParamException if wrong number of operands have been given.
     */
    public function buildBetweenCondition($operator, $operands, &$params)
    {
        if (!isset($operands[0], $operands[1], $operands[2])) {
            throw new InvalidParamException("Operator '$operator' requires three operands.");
        }

        list($column, $value1, $value2) = $operands;

        if (strpos($column, '(') === false) {
            $column = $this->quoteColumnName($column);
        }
        $phName1 = self::PARAM_PREFIX . count($params);
        $params[$phName1] = $value1;
        $phName2 = self::PARAM_PREFIX . count($params);
        $params[$phName2] = $value2;

        return "$column >= @$phName1 && $column <= @$phName2";
    }

    /**
     * @param $operator
     * @param $condition
     * @param $params
     * @return string
     */
    protected function buildLikeCondition($operator, $condition, &$params)
    {
        if (!(isset($condition[0]) && isset($condition[1]))) {
            throw new InvalidParamException("You must set 'column' and 'pattern' params");
        }
        $caseInsensitive = isset($condition[2]) ? (bool)$condition[2] : false;
        return $this->conditionMap[$operator]
            . '('
            . $this->quoteColumnName($condition[0])
            . ', "'
            . $condition[1]
            . '", '
            . ($caseInsensitive ? 'TRUE' : 'FALSE')
            . ')';
    }

    /**
     * @param $columns
     * @return string
     */
    protected function buildOrderBy($columns)
    {
        if (empty($columns)) {
            return '';
        }
        $orders = [];
        foreach ($columns as $name => $direction) {
            $orders[] = $this->quoteColumnName($name) . ($direction === SORT_DESC ? ' DESC' : '');
        }

        return 'SORT ' . implode(', ', $orders);
    }

    /**
     * @param $limit
     * @return bool
     */
    protected function hasLimit($limit)
    {
        return is_string($limit) && ctype_digit($limit) || is_integer($limit) && $limit >= 0;
    }

    /**
     * @param $offset
     * @return bool
     */
    protected function hasOffset($offset)
    {
        return is_integer($offset) && $offset > 0 || is_string($offset) && ctype_digit($offset) && $offset !== '0';
    }

    /**
     * @param $limit
     * @param $offset
     * @return string
     */
    protected function buildLimit($limit, $offset)
    {
        $aql = '';
        if ($this->hasLimit($limit)) {
            $aql = 'LIMIT ' . ($this->hasOffset($offset) ? $offset : '0') . ',' . $limit;
        }

        return $aql;
    }

    /**
     * @param $columns
     * @param $params
     * @return string
     */
    protected function buildSelect($columns, &$params)
    {
        if ($columns === null || empty($columns)) {
            return 'RETURN ' . $this->from;
        }

        if (!is_array($columns)) {
            return 'RETURN ' . $columns;
        }

        $names = '';
        foreach ($columns as $name => $column) {
            $names .= "\"$name\": $this->from.$column, ";
        }

        return 'RETURN {' . trim($names, ', ') . '}';
    }

    /**
     * @param null $query
     * @param array $params
     * @return array
     */
    protected function buildQuery($query = null, $params = [])
    {
        $query = isset($query) ? $query : $this;

        if ($query->where === null) {
            $where = [];
        } else {
            $where = $query->where;
        }

        $params = empty($params) ? $query->params : array_merge($params, $query->params);

        $clauses = [
            $this->buildFrom($query->from),
            $this->buildWhere($where, $params),
            $this->buildOrderBy($query->orderBy, $params),
            $this->buildLimit($query->limit, $query->offset, $params),
            $this->buildSelect($query->select, $params),
        ];

        $aql = implode($query->separator, array_filter($clauses));

        return self::prepareBindVars($aql, $params);
    }

    /**
     * @param Statement $statement
     * @return string
     */
    protected static function getRawAql($statement)
    {
        $query = $statement->getQuery();
        $values = $statement->getBindVars();

        $search = [];
        $replace = [];
        foreach ($values as $key => $value) {
            $search[] = "/@\b$key\b/";
            $replace[] = is_string($value) ? "\"$value\"" : json_encode($value);
        }

        if (count($search)) {
            $query = preg_replace($search, $replace, $query);
        }

        return $query;
    }

    /**
     * @param null $db
     * @return array
     * @throws \Exception
     */
    public function all($db = null)
    {
        $statement = $this->createCommand($db);
        $token = $this->getRawAql($statement);
        Yii::info($token, 'devgroup\arangodb\Query::query');
        try {
            Yii::beginProfile($token, 'devgroup\arangodb\Query::query');
            $cursor = $statement->execute();
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
        } catch (\Exception $ex) {
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
            throw new \Exception($ex->getMessage(), (int) $ex->getCode(), $ex);
        }
        return $this->prepareResult($cursor->getAll());
    }

    /**
     * @param null $db
     * @return array|bool
     * @throws \Exception
     */
    public function one($db = null)
    {
        $this->limit(1);
        $statement = $this->createCommand($db);
        $token = $this->getRawAql($statement);
        Yii::info($token, 'devgroup\arangodb\Query::query');
        try {
            Yii::beginProfile($token, 'devgroup\arangodb\Query::query');
            $cursor = $statement->execute();
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
        } catch (\Exception $ex) {
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
            throw new \Exception($ex->getMessage(), (int) $ex->getCode(), $ex);
        }
        $result = $this->prepareResult($cursor->getAll());
        return empty($result) ? false : $result[0];
    }

    /**
     * @param $collection
     * @param $columns
     * @param array $params
     * @param null $db
     * @return bool
     * @throws \Exception
     */
    public function insert($collection, $columns, $params = [], $db = null)
    {
        $doc = Serializer::encode($columns);

        $clauses = [
            "INSERT $doc IN {$this->quoteCollectionName($collection)}",
            $this->buildOptions(),
        ];

        $aql = implode($this->separator, array_filter($clauses));

        $params = ArrayHelper::merge(
            $params,
            [
                'query' => $aql,
            ]
        );

        $statement = $this->getStatement($params, $db);
        $token = $this->getRawAql($statement);
        Yii::info($token, 'devgroup\arangodb\Query::insert');
        try {
            Yii::beginProfile($token, 'devgroup\arangodb\Query::insert');
            $cursor = $statement->execute();
            Yii::endProfile($token, 'devgroup\arangodb\Query::insert');
        } catch (\Exception $ex) {
            Yii::endProfile($token, 'devgroup\arangodb\Query::insert');
            throw new \Exception($ex->getMessage(), (int) $ex->getCode(), $ex);
        }
        return true;
    }

    /**
     * @param $collection
     * @param $columns
     * @param array $condition
     * @param array $params
     * @param null $db
     * @return bool
     * @throws \Exception
     */
    public function update($collection, $columns, $condition = [], $params = [], $db = null)
    {
        $this->from($collection);
        $clauses = [
            $this->buildFrom($collection),
            $this->buildWhere($condition, $params),
            $this->buildUpdate($collection, $columns),
            $this->buildOptions(),
        ];

        $aql = implode($this->separator, array_filter($clauses));

        $params = ArrayHelper::merge(
            $params,
            [
                'query' => $aql,
                'bindVars' => $params,
            ]
        );

        $statement = $this->getStatement($params, $db);
        $token = $this->getRawAql($statement);
        Yii::info($token, 'devgroup\arangodb\Query::update');
        try {
            Yii::beginProfile($token, 'devgroup\arangodb\Query::update');
            $cursor = $statement->execute();
            Yii::endProfile($token, 'devgroup\arangodb\Query::update');
        } catch (\Exception $ex) {
            Yii::endProfile($token, 'devgroup\arangodb\Query::update');
            throw new \Exception($ex->getMessage(), (int) $ex->getCode(), $ex);
        }
        $meta = $cursor->getMetadata();
        return isset($meta['extra']['operations']['executed']) ?
            $meta['extra']['operations']['executed'] :
            true;
    }

    /**
     * @param $collection
     * @param array $condition
     * @param array $params
     * @param null $db
     * @return bool
     * @throws \Exception
     */
    public function remove($collection, $condition = [], $params = [], $db = null)
    {
        $this->from($collection);
        $clauses = [
            $this->buildFrom($collection),
            $this->buildWhere($condition, $params),
            $this->buildRemove($collection),
            $this->buildOptions(),
        ];

        $aql = implode($this->separator, array_filter($clauses));

        $params = ArrayHelper::merge(
            $params,
            [
                'query' => $aql,
                'bindVars' => $params,
            ]
        );

        $statement = $this->getStatement($params, $db);
        $token = $this->getRawAql($statement);
        Yii::info($token, 'devgroup\arangodb\Query::remove');
        try {
            Yii::beginProfile($token, 'devgroup\arangodb\Query::remove');
            $cursor = $statement->execute();
            Yii::endProfile($token, 'devgroup\arangodb\Query::remove');
        } catch (\Exception $ex) {
            Yii::endProfile($token, 'devgroup\arangodb\Query::remove');
            throw new \Exception($ex->getMessage(), (int) $ex->getCode(), $ex);
        }
        $meta = $cursor->getMetadata();
        return isset($meta['extra']['operations']['executed']) ?
            $meta['extra']['operations']['executed'] :
            true;
    }

    /**
     * @param $collection
     * @param $columns
     * @return string
     */
    protected function buildUpdate($collection, $columns)
    {
        return 'UPDATE ' . $collection . ' WITH '
            . Serializer::encode($columns) . ' IN '
            . $this->quoteCollectionName($collection);
    }

    /**
     * @param $collection
     * @return string
     */
    protected function buildRemove($collection)
    {
        return 'REMOVE ' . $collection . ' IN ' . $collection;
    }

    /**
     * @return string
     */
    protected function buildOptions()
    {
        return empty($this->options) ? '' : ' OPTIONS ' . Json::encode($this->options);
    }

    /**
     * @param Document[] $rows
     * @return array
     */
    public function prepareResult($rows)
    {
        $result = [];
        if (isset($rows[0]) && $rows[0] instanceof Document) {
            if ($this->indexBy === null) {
                foreach ($rows as $row) {
                    $result[] = $row->getAll();
                }
            } else {
                foreach ($rows as $row) {
                    if (is_string($this->indexBy)) {
                        $key = $row->{$this->indexBy};
                    } else {
                        $key = call_user_func($this->indexBy, $row);
                    }
                    $result[$key] = $row->getAll();
                }
            }
        } else {
            $result = $rows;
        }
        return $result;
    }

    /**
     * @param string $q
     * @param null $db
     * @return int
     * @throws \Exception
     * @throws \triagens\ArangoDb\ClientException
     */
    public function count($q = '*', $db = null)
    {
        $this->select = '1';
        $this->limit(1);
        $this->offset(0);
        $statement = $this->createCommand($db);
        $statement->setFullCount(true);
        $statement->setBatchSize(1);

        $token = $this->getRawAql($statement);
        Yii::info($token, 'devgroup\arangodb\Query::query');
        try {
            Yii::beginProfile($token, 'devgroup\arangodb\Query::query');
            $cursor = $statement->execute();
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
        } catch (\Exception $ex) {
            Yii::endProfile($token, 'devgroup\arangodb\Query::query');
            throw new \Exception($ex->getMessage(), (int) $ex->getCode(), $ex);
        }
        return $cursor->getFullCount();
    }

    /**
     * @param null $db
     * @return bool
     * @throws \Exception
     */
    public function exists($db = null)
    {
        $record = $this->one($db);
        return !empty($record);
    }

    /**
     * @param callable|string $column
     * @return $this|static
     */
    public function indexBy($column)
    {
        $this->indexBy = $column;
        return $this;
    }

    /**
     * @param array|string $condition
     * @param array $params
     * @return $this|static
     */
    public function where($condition, $params = [])
    {
        $this->where = $condition;
        $this->addParams($params);
        return $this;
    }

    /**
     * @param array|string $condition
     * @param array $params
     * @return $this|static
     */
    public function andWhere($condition, $params = [])
    {
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['AND', $this->where, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * @param array|string $condition
     * @param array $params
     * @return $this|static
     */
    public function orWhere($condition, $params = [])
    {
        if ($this->where === null) {
            $this->where = $condition;
        } else {
            $this->where = ['OR', $this->where, $condition];
        }
        $this->addParams($params);
        return $this;
    }

    /**
     * Sets the WHERE part of the query but ignores [[isEmpty()|empty operands]].
     *
     * This method is similar to [[where()]]. The main difference is that this method will
     * remove [[isEmpty()|empty query operands]]. As a result, this method is best suited
     * for building query conditions based on filter values entered by users.
     *
     * The following code shows the difference between this method and [[where()]]:
     *
     * ```php
     * // WHERE `age`=:age
     * $query->filterWhere(['name' => null, 'age' => 20]);
     * // WHERE `age`=:age
     * $query->where(['age' => 20]);
     * // WHERE `name` IS NULL AND `age`=:age
     * $query->where(['name' => null, 'age' => 20]);
     * ```
     *
     * Note that unlike [[where()]], you cannot pass binding parameters to this method.
     *
     * @param array $condition the conditions that should be put in the WHERE part.
     * See [[where()]] on how to specify this parameter.
     * @return static the query object itself.
     * @see where()
     * @see andFilterWhere()
     * @see orFilterWhere()
     */
    public function filterWhere(array $condition)
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->where($condition);
        }
        return $this;
    }

    /**
     * Adds an additional WHERE condition to the existing one but ignores [[isEmpty()|empty operands]].
     * The new condition and the existing one will be joined using the 'AND' operator.
     *
     * This method is similar to [[andWhere()]]. The main difference is that this method will
     * remove [[isEmpty()|empty query operands]]. As a result, this method is best suited
     * for building query conditions based on filter values entered by users.
     *
     * @param array $condition the new WHERE condition. Please refer to [[where()]]
     * on how to specify this parameter.
     * @return static the query object itself.
     * @see filterWhere()
     * @see orFilterWhere()
     */
    public function andFilterWhere(array $condition)
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->andWhere($condition);
        }
        return $this;
    }

    /**
     * Adds an additional WHERE condition to the existing one but ignores [[isEmpty()|empty operands]].
     * The new condition and the existing one will be joined using the 'OR' operator.
     *
     * This method is similar to [[orWhere()]]. The main difference is that this method will
     * remove [[isEmpty()|empty query operands]]. As a result, this method is best suited
     * for building query conditions based on filter values entered by users.
     *
     * @param array $condition the new WHERE condition. Please refer to [[where()]]
     * on how to specify this parameter.
     * @return static the query object itself.
     * @see filterWhere()
     * @see andFilterWhere()
     */
    public function orFilterWhere(array $condition)
    {
        $condition = $this->filterCondition($condition);
        if ($condition !== []) {
            $this->orWhere($condition);
        }
        return $this;
    }

    /**
     * Returns a value indicating whether the give value is "empty".
     *
     * The value is considered "empty", if one of the following conditions is satisfied:
     *
     * - it is `null`,
     * - an empty string (`''`),
     * - a string containing only whitespace characters,
     * - or an empty array.
     *
     * @param mixed $value
     * @return boolean if the value is empty
     */
    protected function isEmpty($value)
    {
        return $value === '' || $value === [] || $value === null || is_string($value) && (trim($value) === '' || trim($value, '%') === '');
    }

    /**
     * Removes [[isEmpty()|empty operands]] from the given query condition.
     *
     * @param array $condition the original condition
     * @return array the condition with [[isEmpty()|empty operands]] removed.
     * @throws NotSupportedException if the condition operator is not supported
     */
    protected function filterCondition($condition)
    {
        if (!is_array($condition)) {
            return $condition;
        }

        if (!isset($condition[0])) {
            // hash format: 'column1' => 'value1', 'column2' => 'value2', ...
            foreach ($condition as $name => $value) {
                if ($this->isEmpty($value)) {
                    unset($condition[$name]);
                }
            }
            return $condition;
        }

        // operator format: operator, operand 1, operand 2, ...

        $operator = array_shift($condition);

        switch (strtoupper($operator)) {
            case 'NOT':
            case 'AND':
            case 'OR':
                foreach ($condition as $i => $operand) {
                    $subCondition = $this->filterCondition($operand);
                    if ($this->isEmpty($subCondition)) {
                        unset($condition[$i]);
                    } else {
                        $condition[$i] = $subCondition;
                    }
                }

                if (empty($condition)) {
                    return [];
                }
                break;
            case 'IN':
            case 'LIKE':
                if (array_key_exists(1, $condition) && $this->isEmpty($condition[1])) {
                    return [];
                }
                break;
            case 'BETWEEN':
                if ((array_key_exists(1, $condition) && $this->isEmpty($condition[1]))
                    || (array_key_exists(2, $condition) && $this->isEmpty($condition[2]))) {
                    return [];
                }
                break;
            default:
                throw new NotSupportedException("Operator not supported: $operator");
        }

        array_unshift($condition, $operator);

        return $condition;
    }

    /**
     * @param array|string $columns
     * @return $this|static
     */
    public function orderBy($columns)
    {
        $this->orderBy = $this->normalizeOrderBy($columns);
        return $this;
    }

    /**
     * @param array|string $columns
     * @return $this|static
     */
    public function addOrderBy($columns)
    {
        $columns = $this->normalizeOrderBy($columns);
        if ($this->orderBy === null) {
            $this->orderBy = $columns;
        } else {
            $this->orderBy = array_merge($this->orderBy, $columns);
        }
        return $this;
    }

    /**
     * @param int $limit
     * @return $this|static
     */
    public function limit($limit)
    {
        $this->limit = $limit;
        return $this;
    }

    /**
     * @param int $offset
     * @return $this|static
     */
    public function offset($offset)
    {
        $this->offset = $offset;
        return $this;
    }

    /**
     * @param $columns
     * @return array
     */
    protected function normalizeOrderBy($columns)
    {
        if (is_array($columns)) {
            return $columns;
        } else {
            $columns = preg_split('/\s*,\s*/', trim($columns), -1, PREG_SPLIT_NO_EMPTY);
            $result = [];
            foreach ($columns as $column) {
                if (preg_match('/^(.*?)\s+(asc|desc)$/i', $column, $matches)) {
                    $result[$matches[1]] = strcasecmp($matches[2], 'desc') ? SORT_ASC : SORT_DESC;
                } else {
                    $result[$column] = SORT_ASC;
                }
            }
            return $result;
        }
    }

    /**
     * Sets the parameters to be bound to the query.
     * @param array $params list of query parameter values indexed by parameter placeholders.
     * For example, `[':name' => 'Dan', ':age' => 31]`.
     * @return static the query object itself
     * @see addParams()
     */
    public function params($params)
    {
        $this->params = $params;
        return $this;
    }

    /**
     * Adds additional parameters to be bound to the query.
     * @param array $params list of query parameter values indexed by parameter placeholders.
     * For example, `[':name' => 'Dan', ':age' => 31]`.
     * @return static the query object itself
     * @see params()
     */
    public function addParams($params)
    {
        if (!empty($params)) {
            if (empty($this->params)) {
                $this->params = $params;
            } else {
                foreach ($params as $name => $value) {
                    if (is_integer($name)) {
                        $this->params[] = $value;
                    } else {
                        $this->params[$name] = $value;
                    }
                }
            }
        }
        return $this;
    }

    /**
     * @param $options
     * @return $this
     */
    public function options($options)
    {
        $this->options = $options;
        return $this;
    }

    /**
     * @param $options
     * @return $this
     */
    public function addOptions($options)
    {
        if (!empty($options)) {
            if (empty($this->options)) {
                $this->params = $options;
            } else {
                foreach ($options as $name => $value) {
                    if (is_integer($name)) {
                        $this->options[] = $value;
                    } else {
                        $this->options[$name] = $value;
                    }
                }
            }
        }
        return $this;
    }
}
