<?php defined('SYSPATH') or die('No direct script access.');
/**
 * [Kohana Cache](api/Kohana_Cache) HandlerSocket driver,
 * 
 * ### Configuration example
 * 
 * Below is an example of a _HandlerSocket_ server configuration.
 * 
 *     return array(
 *          'default'   => array(                          // Default group
 *                  'driver'         => 'handlersocket',        // using Memcache driver
 *                  'host'           => 'localhost',
 *                  'port_read'      => 9998,
 *                  'port_write'     => 9999,
 *                  'dbname'         => 'hstestdb',
 *                  'table'          => 'caches',
 *          )
 *     );
 * 
 * In cases where only one cache group is required, if the group is named `default` there is
 * no need to pass the group name when instantiating a cache instance.
 * 
 * #### General cache group configuration settings
 * 
 * Below are the settings available to all types of cache driver.
 * 
 * Name           | Required | Description
 * -------------- | -------- | ---------------------------------------------------------------
 * driver         | __YES__  | (_string_) The driver type to use
 * servers        | __YES__  | (_array_) Associative array of server details, must include a __host__ key. (see _Memcache server configuration_ below)
 * 
 * #### HandlerSocket server configuration
 * 
 * The following settings should be used when defining each handlersocket server
 * 
 * Name             | Required | Description
 * ---------------- | -------- | ---------------------------------------------------------------
 * host             | __YES__  | (_string_) The host of the handlersocket server, i.e. __localhost__; or __127.0.0.1__; or __memcache.domain.tld__
 * port_read        | __YES__  | (_integer_) Point to the port where handlersocket is listening for connections. Read-only. Default __9998__
 * port_write       | __YES__  | (_integer_) Point to the port where memcached is listening for connections. Write-only. Default __9999__
 * dbname           | __YES__  | (_string_) Name of the handlersocket database
 * table            | __YES__  | (_string_) Name of the handlersocket cache table
 * 
 * ### System requirements
 * 
 * *  Kohana 3.x.x
 * *  PHP 5.2.4 or greater
 * *  HandlerSocket plugin for MySQL (or Percona Server) with libhsclient
 * *  HandlerSocket (php-handlersocket) PHP5 Extension
 * 
 * ### SQL Schema
 * 
 *     CREATE TABLE `hstestdb`.`caches` (
 *         `id` VARCHAR(127) CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
 *         `cache` TEXT CHARACTER SET utf8 COLLATE utf8_general_ci NOT NULL,
 *         `expiration` INT(10) NOT NULL,
 *       PRIMARY KEY (`id`)
 *     ) ENGINE = InnoDB DEFAULT CHARSET=utf8;
 *
 * @package    Kohana/Cache
 * @category   Base
 * @version    2.0
 * @author     Kohana Team
 * @copyright  (c) 2009-2010 Kohana Team
 * @license    http://kohanaphp.com/license
 */
class Kohana_Cache_HandlerSocket extends Cache implements Kohana_Cache_GarbageCollect {

	// HandlerSocket has a maximum cache lifetime of 30 days
	const CACHE_CEILING = 2592000;

	/**
	 * HandlerSocket read resource
	 *
	 * @var HandlerSocket
	 */
	protected $_hsread;

	/**
	 * HandlerSocket write resource
	 *
	 * @var HandlerSocket
	 */
	protected $_hswrite;

	/**
	 * Constructs the HandlerSocket Kohana_Cache object
	 *
	 * @param   array     configuration
	 * @throws  Kohana_Cache_Exception
	 */
	protected function __construct(array $config)
	{
		// Check for the HandlerSocket extention
		if ( ! extension_loaded('handlersocket'))
		{
			throw new Kohana_Cache_Exception('HandlerSocket PHP extention not loaded');
		}

		parent::__construct($config);

		// Setup HandlerSocket reader
		$this->_hsread = new HandlerSocket($this->_config['host'], $this->_config['port_read']);

		// Setup HandlerSocket writer
		$this->_hswrite = new HandlerSocket($this->_config['host'], $this->_config['port_write']);
	}

	/**
	 * Retrieve a cached value entry by id.
	 * 
	 *     // Retrieve cache entry from handlersocket group
	 *     $data = Cache::instance('handlersocket')->get('foo');
	 * 
	 *     // Retrieve cache entry from handlersocket group and return 'bar' if miss
	 *     $data = Cache::instance('handlersocket')->get('foo', 'bar');
	 *
	 * @param   string   id of cache to entry
	 * @param   string   default value to return if cache miss
	 * @return  mixed
	 * @throws  Kohana_Cache_Exception
	 */
	public function get($id, $default = NULL)
	{
		if ( ! ($this->_hsread->openIndex(1, $this->_config['dbname'], $this->_config['table'], HandlerSocket::PRIMARY, 'cache,expiration')))
		{
			throw new Kohana_Cache_Exception('There was a problem querying the HandlerSocket cache. :error', array(':error' => $this->_hs->getError()));
		}

		$result = $this->_hsread->executeSingle(1, '=', array($this->_sanitize_id($id)), 1, 0);

		// If the result wasn't found, normalise it
		if ($result === FALSE or ! count($result))
		{
			return $default;
		}
		
		list($value, $expiration) = $result[0];

		if ($expiration != 0 and $expiration <= time())
		{
			// Delete it and return default value
			$this->delete($id);
			return $default;
		}

		// Return the value
		return $value;
	}

	/**
	 * Set a value to cache with id and lifetime
	 * 
	 *     $data = 'bar';
	 * 
	 *     // Set 'bar' to 'foo' in memcache group for 10 minutes
	 *     if (Cache::instance('handlersocket')->set('foo', $data, 600))
	 *     {
	 *          // Cache was set successfully
	 *          return
	 *     }
	 *
	 * @param   string   id of cache entry
	 * @param   mixed    data to set to cache
	 * @param   integer  lifetime in seconds, maximum value 2592000
	 * @return  boolean
	 */
	public function set($id, $data, $lifetime = 3600)
	{
		// If the lifetime is greater than the ceiling
		if ($lifetime > Cache_HandlerSocket::CACHE_CEILING)
		{
			// Set the lifetime to maximum cache time
			$lifetime = Cache_HandlerSocket::CACHE_CEILING + time();
		}
		// Else if the lifetime is greater than zero
		elseif ($lifetime > 0)
		{
			$lifetime += time();
		}
		// Else
		else
		{
			// Normalze the lifetime
			$lifetime = 0;
		}

		if ( ! $this->_hswrite->openIndex(3, $this->_config['dbname'], $this->_config['table'], '', 'id,cache,expiration'))
		{
			throw new Kohana_Cache_Exception('There was a problem querying the HandlerSocket cache. :error', array(':error' => $this->_hs->getError()));
		}

		return $this->_hswrite->executeInsert(3, array($this->_sanitize_id($id), $data, $lifetime));
	}

	/**
	 * Delete a cache entry based on id
	 * 
	 *     // Delete the 'foo' cache entry immediately
	 *     Cache::instance('handlersocket')->delete('foo');
	 * 
	 *     // Delete the 'bar' cache entry after 30 seconds
	 *     Cache::instance('handlersocket')->delete('bar', 30);
	 *
	 * @param   string   id of entry to delete
	 * @param   integer  timeout of entry, if zero item is deleted immediately, otherwise the item will delete after the specified value in seconds
	 * @return  boolean
	 */
	public function delete($id)
	{
		if ( ! ($this->_hswrite->openIndex(4, $this->_config['dbname'], $this->_config['table'], '', 'id')))
		{
			throw new Kohana_Cache_Exception('There was a problem querying the HandlerSocket cache. :error', array(':error' => $this->_hswrite->getError()));
		}

		return (bool) $this->_hswrite->executeDelete(4, '=', array($this->_sanitize_id($id)));
	}

	/**
	 * Delete all cache entries.
	 * 
	 *     // Delete all cache entries in the default group
	 *     Cache::instance('handlersocket')->delete_all();
	 *
	 * @return  boolean
	 */
	public function delete_all()
	{
		if ( ! ($this->_hswrite->openIndex(4, $this->_config['dbname'], $this->_config['table'], '', 'id')))
		{
			throw new Kohana_Cache_Exception('There was a problem querying the HandlerSocket cache. :error', array(':error' => $this->_hswrite->getError()));
		}

		return (bool) $this->_hswrite->executeDelete(4, '!=', array(NULL));
	}
	
	/**
	 * Garbage collection method that cleans any expired
	 * cache entries from the cache.
	 *
	 * @return  boolean
	 */
	public function garbage_collect()
	{
		if ( ! ($this->_hswrite->openIndex(4, $this->_config['dbname'], $this->_config['table'], '', 'expiration')))
		{
			throw new Kohana_Cache_Exception('There was a problem querying the HandlerSocket cache. :error', array(':error' => $this->_hswrite->getError()));
		}

		return (bool) $this->_hswrite->executeDelete(4, '<', array(time()));	
	}

	/**
	 * Close HandlerSocket connection
	 */
	protected function __destruct()
	{
		unset($this->_hsread, $this->_hswrite);
	}
	
}
