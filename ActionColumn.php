<?php

namespace bethrezen\arangodb;

use Yii;
use Closure;
use yii\helpers\Html;
use yii\helpers\Url;

use kartik\icons\Icon;

class ActionColumn extends \yii\grid\Column
{
    public $buttons = [
        [
            'url' => 'edit',
            'icon' => 'pencil',
            'class' => 'btn-primary',
            'label' => 'Edit',
        ],
        [
            'url' => 'delete',
            'icon' => 'trash-o',
            'class' => 'btn-danger',
            'label' => 'Delete',
        ],
    ];
    /**
     * @var string the ID of the controller that should handle the actions specified here.
     * If not set, it will use the currently active controller. This property is mainly used by
     * [[urlCreator]] to create URLs for different actions. The value of this property will be prefixed
     * to each action name to form the route of the action.
     */
    public $controller;
    /**
     * @var callable a callback that creates a button URL using the specified model information.
     * The signature of the callback should be the same as that of [[createUrl()]].
     * If this property is not set, button URLs will be created using [[createUrl()]].
     */
    public $urlCreator;

    /**
     * Creates a URL for the given action and model.
     * This method is called for each button and each row.
     * @param string $action the button name (or action ID)
     * @param \yii\db\ActiveRecord $model the data model
     * @param mixed $key the key associated with the data model
     * @param integer $index the current row index
     * @return string the created URL
     */
    public function createUrl($action, $model, $key, $index)
    {
        if ($this->urlCreator instanceof Closure) {
            return call_user_func($this->urlCreator, $action, $model, $key, $index);
        } else {
            $params = is_array($key) ? $key : ['id' => (string) $key];
            $params[0] = $this->controller ? $this->controller . '/' . $action : $action;

            return Url::toRoute($params);
        }
    }


    protected function renderDataCellContent($model, $key, $index)
    {
        $data = '';
        foreach ($this->buttons as $button) {
            $data .= Html::a(
                Icon::show($button['icon']).'&nbsp;&nbsp;'.$button['label'],
                $url = $this->createUrl($button['url'], $model, $key, $index),
                [
                    'data-pjax' => 0,
                    'class' => 'btn btn-xs '.$button['class']
                ]
            ) . ' ';
        }

        return $data;
    }
}
