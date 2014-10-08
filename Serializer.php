<?php
/**
 * User: evgen-d
 * Date: 08.10.14
 * Time: 16:04
 */

namespace devgroup\arangodb;

use yii\base\Arrayable;

class Serializer
{
    public static function encode($value, $options = 0)
    {
        $expressions = [];
        $value = static::processData($value, $expressions, uniqid());
        $json = json_encode($value, $options);

        return empty($expressions) ? $json : strtr($json, $expressions);
    }

    protected static function processData($data, &$expressions, $expPrefix)
    {
        if (is_object($data)) {
            if ($data instanceof AqlExpression) {
                $token = "!{[$expPrefix=" . count($expressions) . ']}!';
                $expressions['"' . $token . '"'] = $data->expression;

                return $token;
            } elseif ($data instanceof \JsonSerializable) {
                $data = $data->jsonSerialize();
            } elseif ($data instanceof Arrayable) {
                $data = $data->toArray();
            } else {
                $result = [];
                foreach ($data as $name => $value) {
                    $result[$name] = $value;
                }
                $data = $result;
            }

            if ($data === []) {
                return new \stdClass();
            }
        }

        if (is_array($data)) {
            foreach ($data as $key => $value) {
                if (is_array($value) || is_object($value)) {
                    $data[$key] = static::processData($value, $expressions, $expPrefix);
                }
            }
        }

        return $data;
    }
}
 