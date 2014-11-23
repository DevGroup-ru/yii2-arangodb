ArangoDb Extension for Yii 2
===========================

This extension provides the [ArangoDB](http://www.arangodb.org/) integration for the Yii2 framework.


Installation
------------

This extension requires [ArangoDB PHP Extension](https://github.com/triAGENS/ArangoDB-PHP)

The preferred way to install this extension is through [composer](http://getcomposer.org/download/).

Either run

```
php composer.phar require --prefer-dist devgroup/yii2-arangodb "*"
```

or add

```
"devgroup/yii2-arangodb": "*"
```

to the require section of your composer.json.


General Usage
-------------

To use this extension, simply add the following code in your application configuration:

```php
return [
    //....
    'components' => [
        'arangodb' => [
            'class' => '\devgroup\arangodb\Connection',
            'connectionOptions' => [
                triagens\ArangoDb\ConnectionOptions::OPTION_DATABASE => "mydatabase",
                triagens\ArangoDb\ConnectionOptions::OPTION_ENDPOINT => 'tcp://127.0.0.1:8529',
                //triagens\ArangoDb\ConnectionOptions::OPTION_AUTH_USER   => '',
                //triagens\ArangoDb\ConnectionOptions::OPTION_AUTH_PASSWD => '',
            ],
        ],
    ],
];
```

Using the connection instance you may access databases, collections and documents.

To perform "find" queries, you should use [[\devgroup\arangodb\Query]]:

```php
use devgroup\arangodb\Query;

$query = new Query;
// compose the query
$query->select(['name', 'status'])
    ->from('customer')
    ->limit(10);
// execute the query
$rows = $query->all();
```


Using the ArangoDB ActiveRecord
------------------------------

This extension provides ActiveRecord solution similar ot the [[\yii\db\ActiveRecord]].
To declare an ActiveRecord class you need to extend [[\devgroup\arangodb\ActiveRecord]] and
implement the `collectionName` and 'attributes' methods:

```php
use devgroup\arangodb\ActiveRecord;

class Customer extends ActiveRecord
{
    /**
     * @return string the name of the index associated with this ActiveRecord class.
     */
    public static function collectionName()
    {
        return 'customer';
    }

    /**
     * @return array list of attribute names.
     */
    public function attributes()
    {
        return ['_key', 'name', 'email', 'address', 'status'];
    }
}
```

Note: collection primary key name ('_key') should be always explicitly setup as an attribute.

You can use [[\yii\data\ActiveDataProvider]] with [[\devgroup\arangodb\Query]] and [[\devgroup\arangodb\ActiveQuery]]:

```php
use yii\data\ActiveDataProvider;
use devgroup\arangodb\Query;

$query = new Query;
$query->from('customer')->where(['status' => 2]);
$provider = new ActiveDataProvider([
    'query' => $query,
    'pagination' => [
        'pageSize' => 10,
    ]
]);
$models = $provider->getModels();
```

```php
use yii\data\ActiveDataProvider;
use app\models\Customer;

$provider = new ActiveDataProvider([
    'query' => Customer::find(),
    'pagination' => [
        'pageSize' => 10,
    ]
]);
$models = $provider->getModels();
```


Using Migrations
----------------

ArangoDB migrations are managed via [[devgroup\arangodb\console\controllers\MigrateController]], which is an analog of regular
[[\yii\console\controllers\MigrateController]].

In order to enable this command you should adjust the configuration of your console application:

```php
return [
    // ...
    'controllerMap' => [
        'arangodb-migrate' => 'devgroup\arangodb\console\controllers\MigrateController'
    ],
];
```

Below are some common usages of this command:

```
# creates a new migration named 'create_user_collection'
yii arangodb-migrate/create create_user_collection

# applies ALL new migrations
yii arangodb-migrate

# reverts the last applied migration
yii arangodb-migrate/down
```


Using Debug Panel
-----------------

Add ArangoDb panel to your yii\debug\Module configuration

```php
return [
    'bootstrap' => ['debug'],
    'modules' => [
        'debug' => 'yii\debug\Module',
        'panels' => [
            'arango' => [
                'class' => 'devgroup\arangodb\panels\arangodb\ArangoDbPanel',
            ],
        ],
        ...
    ],
    ...
];
```


[![Bitdeli Badge](https://d2weczhvl823v0.cloudfront.net/DevGroup-ru/yii2-arangodb/trend.png)](https://bitdeli.com/free "Bitdeli Badge")

