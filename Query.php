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
     * @param $fields
     * @return $this
     */
    public function select($fields)
    {
        $this->select = $fields;

        return $this;
    }

    public function from($collection)
    {
        $this->from = $collection;

        return $this;
    }

    protected function buildFrom($collection)
    {
        $collection = trim($collection);
        return $collection ? "FOR $collection IN $collection" : '';
    }

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

    protected function buildWhere($condition, &$params)
    {
        $where = $this->buildCondition($condition, $params);

        return $where === '' ? '' : 'FILTER ' . $where;
    }

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

    protected function hasLimit($limit)
    {
        return is_string($limit) && ctype_digit($limit) || is_integer($limit) && $limit >= 0;
    }

    protected function hasOffset($offset)
    {
        return is_integer($offset) && $offset > 0 || is_string($offset) && ctype_digit($offset) && $offset !== '0';
    }

    protected function buildLimit($limit, $offset)
    {
        $aql = '';
        if ($this->hasLimit($limit)) {
            $aql = 'LIMIT ' . ($this->hasOffset($offset) ? $offset : '0') . ',' . $limit;
        }

        return $aql;
    }

    protected function buildSelect($columns, &$params)
    {
        if ($columns == null || empty($columns)) {
            return 'RETURN ' . $this->from;
        }

        if (!is_array($columns)) {
            return 'RETURN ' . $columns;
        }

        $names = '';
        foreach ($columns as $name => $column) {
            if (is_int($name)) {
                $names .= $column . ', ';
            } else {
                $names .= "\"$name\": $this->from.$column, ";
            }
        }

        return 'RETURN {' . trim($names, ', ') . '}';
    }

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

        return [$aql, $params];
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
            $search[] = "@$key";
            $replace[] = is_string($value) ? "\"$value\"" : $value;
        }

        if (count($search)) {
            $query = str_replace($search, $replace, $query);
        }

        return $query;
    }

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

    public function insert($collection, $columns, $params = [], $db = null)
    {
        $doc = Json::encode($columns);

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

    protected function buildUpdate($collection, $columns)
    {
        return 'UPDATE ' . $collection . ' WITH '
            . (is_array($columns) ? Json::encode($columns) : $columns) . ' IN '
            . $this->quoteCollectionName($collection);
    }

    protected function buildRemove($collection)
    {
        return 'REMOVE ' . $collection . ' IN ' . $collection;
    }

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
        return $result;
    }

    public function count($q = '*', $db = null)
    {
        $statement = $this->createCommand($db);
        $statement->setCount(true);
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
        return $cursor->getCount();
    }

    public function exists($db = null)
    {
        $record = $this->one($db);
        return !empty($record);
    }

    public function indexBy($column)
    {
        $this->indexBy = $column;
        return $this;
    }

    public function where($condition, $params = [])
    {
        $this->where = $condition;
        $this->addParams($params);
        return $this;
    }

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
        return $value === '' || $value === [] || $value === null || is_string($value) && trim($value) === '';
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
            default:
                throw new NotSupportedException("Operator not supported: $operator");
        }

        array_unshift($condition, $operator);

        return $condition;
    }

    public function orderBy($columns)
    {
        $this->orderBy = $this->normalizeOrderBy($columns);
        return $this;
    }

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

    public function limit($limit)
    {
        $this->limit = $limit;
        return $this;
    }

    public function offset($offset)
    {
        $this->offset = $offset;
        return $this;
    }

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

    public function options($options)
    {
        $this->options = $options;
        return $this;
    }

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
