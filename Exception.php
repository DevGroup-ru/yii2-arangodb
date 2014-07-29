<?php

namespace devgroup\arangodb;

class Exception extends \yii\base\Exception
{
    /**
     * @return string the user-friendly name of this exception
     */
    public function getName()
    {
        return 'ArangoDB Exception';
    }
}
