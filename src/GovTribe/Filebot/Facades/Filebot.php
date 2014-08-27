<?php namespace GovTribe\Filebot\Facades;

use Illuminate\Support\Facades\Facade;

class Filebot extends Facade
{
    /**
     * Get the registered name of the component.
     *
     * @return string
     */
    protected static function getFacadeAccessor()
    {
        return 'filebot';
    }
}
