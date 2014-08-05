<?php
/* @var $panel \devgroup\arangodb\panels\arangodb\ArangoDbPanel */
/* @var $queryCount integer */
/* @var $queryTime integer */
?>
<?php if ($queryCount): ?>
    <div class="yii-debug-toolbar-block">
        <a href="<?= $panel->getUrl() ?>" title="Executed <?= $queryCount ?> database queries which took <?= $queryTime ?>.">
            Ar. DB <span class="label label-info"><?= $queryCount ?></span> <span class="label"><?= $queryTime ?></span>
        </a>
    </div>
<?php endif; ?>