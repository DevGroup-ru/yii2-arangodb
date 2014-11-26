<?php

namespace devgroup\arangodb;

use Yii;
use yii\base\InvalidConfigException;
use yii\db\ActiveQueryInterface;
use yii\db\BaseActiveRecord;
use yii\db\StaleObjectException;
use yii\helpers\ArrayHelper;
use yii\helpers\Inflector;
use yii\helpers\StringHelper;

use triagens\ArangoDb\Document;

abstract class ActiveRecord extends BaseActiveRecord
{
    public function mergeAttribute($name, $value)
    {
        $newValue = $this->getAttribute($name);
        if (!is_array($newValue)) {
            $newValue === null ? [] : [$newValue];
        }

        if (is_array($value)) {
            $this->setAttribute($name, ArrayHelper::merge($newValue, $value));
        } else {
            $newValue[] = $value;
            $this->setAttribute($name, $newValue);
        }
    }

    public static function collectionName()
    {
        return Inflector::camel2id(StringHelper::basename(get_called_class()), '_');
    }

    /**
     * Returns the primary key **name(s)** for this AR class.
     *
     * Note that an array should be returned even when the record only has a single primary key.
     *
     * For the primary key **value** see [[getPrimaryKey()]] instead.
     *
     * @return string[] the primary key name(s) for this AR class.
     */
    public static function primaryKey()
    {
        return ['_key'];
    }

    /**
     * Creates an [[ActiveQueryInterface|ActiveQuery]] instance for query purpose.
     *
     * The returned [[ActiveQueryInterface|ActiveQuery]] instance can be further customized by calling
     * methods defined in [[ActiveQueryInterface]] before `one()` or `all()` is called to return
     * populated ActiveRecord instances. For example,
     *
     * ```php
     * // find the customer whose ID is 1
     * $customer = Customer::find()->where(['id' => 1])->one();
     *
     * // find all active customers and order them by their age:
     * $customers = Customer::find()
     *     ->where(['status' => 1])
     *     ->orderBy('age')
     *     ->all();
     * ```
     *
     * This method is also called by [[BaseActiveRecord::hasOne()]] and [[BaseActiveRecord::hasMany()]] to
     * create a relational query.
     *
     * You may override this method to return a customized query. For example,
     *
     * ```php
     * class Customer extends ActiveRecord
     * {
     *     public static function find()
     *     {
     *         // use CustomerQuery instead of the default ActiveQuery
     *         return new CustomerQuery(get_called_class());
     *     }
     * }
     * ```
     *
     * The following code shows how to apply a default condition for all queries:
     *
     * ```php
     * class Customer extends ActiveRecord
     * {
     *     public static function find()
     *     {
     *         return parent::find()->where(['deleted' => false]);
     *     }
     * }
     *
     * // Use andWhere()/orWhere() to apply the default condition
     * // FOR customer IN customer FILTER customer.deleted=:deleted AND customer.age>30 RETURN customer
     * $customers = Customer::find()->andWhere('age>30')->all();
     *
     * // Use where() to ignore the default condition
     * // FOR customer IN customer FILTER customer.age>30 RETURN customer
     * $customers = Customer::find()->where('age>30')->all();
     *
     * @return ActiveQueryInterface the newly created [[ActiveQueryInterface|ActiveQuery]] instance.
     */
    public static function find()
    {
        /** @var ActiveQuery $query */
        $query = \Yii::createObject(ActiveQuery::className(), [get_called_class()]);
        $query->from(static::collectionName())->select(static::collectionName());

        return $query;
    }

    /**
     * @param ActiveRecord $record
     * @param Document|array $row
     */
    public static function populateRecord($record, $row)
    {
        if ($row instanceof Document) {
            $row = $row->getAll();
        }

        parent::populateRecord($record, $row);
    }

    public function attributes()
    {
        $class = new \ReflectionClass($this);
        $names = [];
        foreach ($class->getProperties(\ReflectionProperty::IS_PUBLIC) as $property) {
            if (!$property->isStatic()) {
                $names[] = $property->getName();
            }
        }

        return $names;
    }

    /**
     * Inserts the record into the database using the attribute values of this record.
     *
     * Usage example:
     *
     * ```php
     * $customer = new Customer;
     * $customer->name = $name;
     * $customer->email = $email;
     * $customer->insert();
     * ```
     *
     * @param boolean $runValidation whether to perform validation before saving the record.
     * If the validation fails, the record will not be inserted into the database.
     * @param array $attributes list of attributes that need to be saved. Defaults to null,
     * meaning all attributes that are loaded from DB will be saved.
     * @param array $options
     * @return boolean whether the attributes are valid and the record is inserted successfully.
     */
    public function insert($runValidation = true, $attributes = null, $options = [])
    {
        if ($runValidation && !$this->validate($attributes)) {
            return false;
        }
        $result = $this->insertInternal($attributes, $options);

        return $result;
    }

    protected function insertInternal($attributes = null, $options = [])
    {
        if (!$this->beforeSave(true)) {
            return false;
        }
        $values = $this->getDirtyAttributes($attributes);
        if (empty($values)) {
            $currentAttributes = $this->getAttributes();
            foreach ($this->primaryKey() as $key) {
                $values[$key] = isset($currentAttributes[$key]) ? $currentAttributes[$key] : null;
            }
        }

        $newId = static::getDb()->getDocumentHandler()->save(static::collectionName(), $values);
        static::populateRecord($this, static::getDb()->getDocument(static::collectionName(), $newId));
        $this->setIsNewRecord(false);

        $changedAttributes = array_fill_keys(array_keys($values), null);
        $this->setOldAttributes($this->getAttributes());
        $this->afterSave(true, $changedAttributes);

        return true;
    }

    public function update($runValidation = true, $attributeNames = null, $options = [])
    {
        if ($runValidation && !$this->validate($attributeNames)) {
            return false;
        }
        return $this->updateInternal($attributeNames, $options);
    }

    protected function updateInternal($attributes = null, $options = [])
    {
        if (!$this->beforeSave(false)) {
            return false;
        }
        $values = $this->getDirtyAttributes($attributes);
        if (empty($values)) {
            $this->afterSave(false, $values);
            return 0;
        }
        $condition = $this->getOldPrimaryKey(true);
        $lock = $this->optimisticLock();
        if ($lock !== null) {
            if (!isset($values[$lock])) {
                $values[$lock] = $this->$lock + 1;
            }
            $condition[$lock] = $this->$lock;
        }

        foreach ($values as $key => $attribute) {
            $this->setAttribute($key, $attribute);
        }

        $rows = (new Query())->options($options)->update(
            static::collectionName(),
            $values,
            [
                '_key' => $this->getOldAttribute('_key'),
            ]
        );

        if ($lock !== null && !$rows) {
            throw new StaleObjectException('The object being updated is outdated.');
        }

        $changedAttributes = [];
        foreach ($values as $name => $value) {
            $changedAttributes[$name] = $this->getOldAttribute($name);
            $this->setOldAttribute($name, $value);
        }
        $this->afterSave(false, $changedAttributes);

        return $rows;
    }

    /**
     * Returns the connection used by this AR class.
     * @return Connection the database connection used by this AR class.
     */
    public static function getDb()
    {
        return \Yii::$app->get('arangodb');
    }

    protected static function findByCondition($condition, $one)
    {
        /** @var ActiveQuery $query */
        $query = static::find();

        if (!ArrayHelper::isAssociative($condition)) {
            // query by primary key
            $primaryKey = static::primaryKey();
            if (isset($primaryKey[0])) {
                $collection = static::collectionName();
                $condition = ["{$collection}.{$primaryKey[0]}" => $condition];
            } else {
                throw new InvalidConfigException(get_called_class() . ' must have a primary key.');
            }
        }

        return $one ? $query->andWhere($condition)->one() : $query->andWhere($condition)->all();
    }

    /**
     * Updates records using the provided attribute values and conditions.
     * For example, to change the status to be 1 for all customers whose status is 2:
     *
     * ~~~
     * Customer::updateAll(['status' => 1], ['status' => '2']);
     * ~~~
     *
     * @param array $attributes attribute values (name-value pairs) to be saved for the record.
     * Unlike [[update()]] these are not going to be validated.
     * @param array $condition the condition that matches the records that should get updated.
     * Please refer to [[QueryInterface::where()]] on how to specify this parameter.
     * An empty condition will match all records.
     * @param array $options
     * @return integer the number of rows updated
     */
    public static function updateAll($attributes, $condition = [], $options = [])
    {
        return (new Query())->options($options)->update(static::collectionName(), $attributes, $condition);
    }

    /**
     * Deletes records using the provided conditions.
     * WARNING: If you do not specify any condition, this method will delete ALL rows in the table.
     *
     * For example, to delete all customers whose status is 3:
     *
     * ~~~
     * Customer::deleteAll([status = 3]);
     * ~~~
     *
     * @param array $condition the condition that matches the records that should get deleted.
     * Please refer to [[QueryInterface::where()]] on how to specify this parameter.
     * An empty condition will match all records.
     * @param array $options
     * @return integer the number of rows deleted
     */
    public static function deleteAll($condition = [], $options = [])
    {
        return (new Query())->options($options)->remove(static::collectionName(), $condition);
    }

    public static function truncate()
    {
        return static::getDb()->getCollectionHandler()->truncate(static::collectionName());
    }

    /**
     * Saves the current record.
     *
     * This method will call [[insert()]] when [[getIsNewRecord()|isNewRecord]] is true, or [[update()]]
     * when [[getIsNewRecord()|isNewRecord]] is false.
     *
     * For example, to save a customer record:
     *
     * ~~~
     * $customer = new Customer; // or $customer = Customer::findOne($id);
     * $customer->name = $name;
     * $customer->email = $email;
     * $customer->save();
     * ~~~
     *
     * @param boolean $runValidation whether to perform validation before saving the record.
     * If the validation fails, the record will not be saved to database. `false` will be returned
     * in this case.
     * @param array $attributeNames list of attributes that need to be saved. Defaults to null,
     * meaning all attributes that are loaded from DB will be saved.
     * @param array $options
     * @return boolean whether the saving succeeds
     */
    public function save($runValidation = true, $attributeNames = null, $options = [])
    {
        if ($this->getIsNewRecord()) {
            return $this->insert($runValidation, $attributeNames, $options);
        } else {
            return $this->update($runValidation, $attributeNames, $options) !== false;
        }
    }

    /**
     * Deletes the record from the database.
     *
     * @param array $options
     * @return integer|boolean the number of rows deleted, or false if the deletion is unsuccessful for some reason.
     * Note that it is possible that the number of rows deleted is 0, even though the deletion execution is successful.
     */
    public function delete($options = [])
    {
        $result = false;
        if ($this->beforeDelete()) {
            $result = $this->deleteInternal($options);
            $this->afterDelete();
        }

        return $result;
    }

    /**
     * @see ActiveRecord::delete()
     * @throws StaleObjectException
     */
    protected function deleteInternal($options = [])
    {
        $condition = $this->getOldPrimaryKey(true);
        $lock = $this->optimisticLock();
        if ($lock !== null) {
            $condition[$lock] = $this->$lock;
        }
        $result = (new Query())->options($options)->remove(static::collectionName(), $condition);
        if ($lock !== null && !$result) {
            throw new StaleObjectException('The object being deleted is outdated.');
        }
        $this->setOldAttributes(null);

        return $result;
    }

    /**
     * Returns a value indicating whether the named attribute has been changed.
     * @param string $name the name of the attribute
     * @return boolean whether the attribute has been changed
     */
    public function isAttributeChanged($name, $depth = 2)
    {
        if (is_array($this->getAttribute($name))) {
            $new = $this->getAttribute($name);
            $old = $this->getOldAttribute($name);
            if ($depth < 1) {
                $depth = 1;
            }
            return self::isArrayChanged($new, $old, $depth);
        } else {
            return parent::isAttributeChanged($name);
        }
    }

    private static function isArrayChanged(&$new, &$old, $depth)
    {
        if (is_array($new)) {
            if (is_array($old)) {
                if (count($new) != count($old)) {
                    return true;
                } else {
                    $newKeys = array_keys($new);
                    $oldKeys = array_keys($old);
                    if (array_merge(array_diff($newKeys, $oldKeys), array_diff($oldKeys, $newKeys))) {
                        return true;
                    } else {
                        if ($depth > 1) {
                            foreach ($new as $key => $value) {
                                if (self::isArrayChanged($new[$key], $old[$key], $depth--)) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            } else {
                return true;
            }
        } else {
            if (is_array($old)) {
                return true;
            } else {
                return (string)$new != (string)$old;
            }
        }

        return false;
    }

    public function init()
    {
        parent::init();
        if ($this->scenario === static::SCENARIO_DEFAULT) {
            $this->setAttributes($this->defaultValues(), false);
        }
    }

    public function defaultValues()
    {
        return [];
    }
}
