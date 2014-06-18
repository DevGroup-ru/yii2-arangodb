<?php

namespace devgroup\arangodb;

use yii;


use triagens\ArangoDb\Document;

class ArangoModel extends \yii\base\Model {

    private $_isNewRecord = true;

    private $_doc = null;

    public $_id = null;

    public static function findById($id)
    {
        $parts = explode("\\", $id);
        if (count($parts)==2) {
            $id = $parts[1]; // для формата "Collection\1237643123"
        } else {
            $parts = explode("/", $id); // для формата "Collection/123123321"
            if (count($parts)==2) {
                $id = $parts[1];
            }
        }
        $model = new static;
        $model
            ->setDocument(Yii::$app->arango->getDocument(static::class_to_collection(get_called_class()), $id))
            ->setIsNewRecord(false);

        return $model;
    }

    public function getAttributes($names=null, $except=['_id']){
        return parent::getAttributes($names, $except);
    }

    /**
     * @todo функция должна возвращать true/false в зависимости от результата
     * Но аранга возвращает различный тип данных. Надо написать код
     *
     */
    public function save()
    {
        if ($this->_isNewRecord) {
            // добавляем запись
            $this->_doc = Document::createFromArray($this->getAttributes());

            $result = intval(Yii::$app->arango->documentHandler()->add(static::class_to_collection(get_called_class()), $this->_doc)) > 0;
            if ($result) {
                $this->_isNewRecord = false;
            }
            return $result;
        } else {
            // патчим!
            $doc_attributes = array_keys($this->_doc->getAll());

            $attributes = $this->getAttributes();
            foreach ($attributes as $k=>$v) {
                $this->_doc->set($k, $v);
                unset($doc_attributes[$k]);
            }
            foreach ($doc_attributes as $key) {
                if ($key != '_key')
                    unset($this->_doc->$key);
            }
            return Yii::$app->arango->documentHandler()->update($this->_doc);
        }
    }

    private static function class_to_collection($class)
    {
        $parts = explode("\\", $class);
        return end($parts);
    }
    private static function id_to_int($class)
    {
        $parts = explode("/", $class);
        return end($parts);
    }

    public function setIsNewRecord($state)
    {
        $this->_isNewRecord = $state;
        return $this;
    }

    public function setDocument($doc)
    {
        $this->_doc = $doc;
        $all = $this->_doc->getAll();
        $this->_id = $this->_doc->getInternalId();
        $this->setAttributes($all, false);
        
        return $this;
    }

    public function delete()
    {

        Yii::$app->arango->documentHandler()->deleteById(
            static::class_to_collection(get_called_class()), 
            static::id_to_int($this->_doc->getInternalId())
        );
    }
}