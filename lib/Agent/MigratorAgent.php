<?php

namespace BitrixMigrator\Agent;

use BitrixMigrator\Config\Module;

final class MigratorAgent
{
    /**
     * Агент миграции.
     *
     * Контракт агентов Битрикса: метод должен вернуть строку вызова самого себя,
     * чтобы агент продолжил выполняться, или пустую строку для самоудаления.
     */
    public static function run(): string
    {
        // На этапе каркаса ничего не делаем.
        // Следующая итерация: запуск TaskRunner и обработка HL-очереди.

        return "\\" . __CLASS__ . "::run();";
    }
}
