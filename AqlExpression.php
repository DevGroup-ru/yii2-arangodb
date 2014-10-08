<?php
/**
 * User: evgen-d
 * Date: 08.10.14
 * Time: 16:00
 */

namespace devgroup\arangodb;

use yii\base\Object;

class AqlExpression extends Object
{
    /**
     * @var string the AQL expression represented by this object
     */
    public $expression;


    /**
     * Constructor.
     * @param string $expression the AQL expression represented by this object
     * @param array $config additional configurations for this object
     */
    public function __construct($expression, $config = [])
    {
        $this->expression = $expression;
        parent::__construct($config);
    }

    /**
     * The PHP magic function converting an object into a string.
     * @return string the AQL expression.
     */
    public function __toString()
    {
        return $this->expression;
    }
}
 