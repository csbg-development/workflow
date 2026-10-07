<?php

declare(strict_types=1);

namespace Workflow\V2\Support;

use Illuminate\Database\Eloquent\Builder;
use Illuminate\Database\Eloquent\Model;

final class RunWaitOrder
{
    /**
     * @template TModel of Model
     * @param Builder<TModel> $query
     * @return Builder<TModel>
     */
    public static function applyToQuery(Builder $query): Builder
    {
        $driver = $query->getQuery()
            ->getConnection()
            ->getDriverName();

        [$openedAtOrder, $waitIdOrder] = match ($driver) {
            'mysql', 'mariadb' => ["LEFT(DATE_FORMAT(opened_at, '%Y-%m-%d %H:%i:%s.%f'), 23)", 'BINARY wait_id'],
            'pgsql' => ["date_trunc('milliseconds', opened_at)", 'wait_id COLLATE "C"'],
            'sqlite' => ['substr(opened_at, 1, 23)', 'wait_id COLLATE BINARY'],
            'sqlsrv' => ['CONVERT(varchar(23), opened_at, 121)', 'wait_id COLLATE Latin1_General_100_BIN2'],
            default => ['opened_at', 'wait_id'],
        };

        return $query
            ->orderByRaw("CASE status WHEN 'open' THEN 0 WHEN 'resolved' THEN 1 WHEN 'cancelled' THEN 2 ELSE 99 END")
            ->orderByRaw('CASE WHEN opened_at IS NULL THEN 1 ELSE 0 END')
            ->orderByRaw($openedAtOrder)
            ->orderByRaw($waitIdOrder);
    }
}
