<?php defined('SYSPATH') or die('No direct script access.');
/**
 * ORM-like wrapper for HandlerSocket
 * 
 * @author Michael Lavers <kolanos@gmail.com>
 */
class HS_Core {

	/**
	 * HandlerSocket modes
	 */
	const SELECT = 1;
	const UPDATE = 2;
	const INSERT = 3;
	const DELETE = 4;
	const 

	/**
	 * @var  string  default HandlerSocket group
	 */
	public static $default = 'default';

	/**
	 * @var array Instances of HandlerSocket
	 */
	protected static $_instances = array();

	/**
	 * Create an instance of HS (HandlerSocket wrapper)
	 * 
	 * @param	string	Configuration group
	 * @return	HS
	 */
	public static function instance($group = NULL)
	{
		if ($group === NULL)
		{
			// Use the default type
			$group = HS::$default;
		}
	
		if ( ! isset(self::$_instances[$group]))
		{
			// Load the configuration for this group
			$config = Kohana::config('hs')->get($group);
		
			self::$_instances[$group] = new self($config);
		}
		
		return self::$_instances[$group];
	}

	/**
	 * @var  Config
	 */
	protected $_config;

	/**
	 * @var  HandlerSocket (read-only)
	 */
	protected $_hsread;
	
	/**
	 * @var  HandlerSocket (write-only)
	 */
	protected $_hswrite;

	/**
	 * Constructor
	 * 
	 * @throws HS_Exception
	 * @param   array   configuration
	 * @return  void
	 */
	public function __construct(array $config = NULL)
	{
		// Check for the HandlerSocket extention
		if ( ! extension_loaded('handlersocket'))
		{
			throw new HS_Exception('HandlerSocket PHP extention not loaded');
		}
		
		// Check for a configuration
		if ($config === NULL)
		{
			throw new HS_Exception('HandlerSocket configuration missing');
		}
	
		$this->_config = $config;
				
		// Two separate instances of HandlerSocket needed to read and write
		$this->_hsread = new HandlerSocket($this->_config['host'], $this->_config['port_read']);
		$this->_hswrite = new HandlerSocket($this->_config['host'], $this->_config['port_write']);
	}
		
	/**
	 * @var array Fields to select
	 */
	protected $_select = array();
	
	/**
	 * Select fields to get(), update() or insert()
	 * 
	 * Usage:
	 * 
	 * HS::instance()->select('field1', 'field2', 'field3');
	 * 
	 * @chainable
	 * @param array Fields to select
	 * @return object
	 */
	public function select($columns = NULL)
	{
		$columns = func_get_args();

		$this->_select = array_merge($this->_select, $columns);
	
		return $this;
	}

	/**
	 * @var string Table to query
	 */
	protected $_from;
	
	/**
	 * Select table to query
	 * 
	 * Usage:
	 * 
	 * HS::instance()->from('hstable');
	 *
	 * @chainable
	 * @param   mixed  Table name or array($table, $alias) or object
	 * @param   ...
	 * @return  $this
	 */
	public function from($table)
	{
		$this->_from = (string) $table;

		return $this;
	}
	
	/**
	 * @var array Where statement
	 */
	protected $_where = array();

	/**
	 * @var array Supported operators
	 */
	protected $_supported_operators = array('=', '>=', '<=', '>', '<', '+');
	
	/**
	 * Basic WHERE synxtax, dependent on index
	 * 
	 * Usage:
	 * 
	 * HS::instance()->where('id', '=', array('1', '2', ...));
	 * 
	 * Note: Only one where() statement can be used, calling it again in the 
	 * same instance it will be overwritten. Only one operator can be used per
	 * query.
	 * 
	 * @chainable
	 * @param string Index to use for condition, defaul 'PRIMARY' (primary key)
	 * @param string Operator to use for condition (defauls to '=')
	 * @param array Values to compare condition against
	 * @return $this
	 */
	public function where($index = NULL, $operator = NULL, array $values = NULL)
	{
		if ($index === NULL)
		{
			$index = HandlerSocket::PRIMARY;
		}

		if ($operator === NULL or ! in_array($operator, $this->_supported_operators))
		{
			$operator = '=';
		}
		
		$this->_where = array($index, $operator, $values);
	
		return $this;
	}
	
	/**
	 * Perform select
	 * 
	 * Usage:
	 * 
	 * HS::instance()->select('field1', 'field2', 'field3')
	 * 		->from('hstable')
	 * 		->where('id', '=', array('1'))
	  * 	->get();
	 * 
	 * @throws HS_Exception
	 * @param integer Limit number of results
	 * @param integer Skip number of results
	 * @return array|boolean An array of rows or FALSE (if none found).
	 */
	public function get($limit = NULL, $offset = NULL)
	{
		if ( ! $this->_hsread->openIndex(HS::SELECT, $this->_config['dbname'], $this->_from, $this->_where[0], implode(',', $this->_select)))
		{
			throw new HS_Exception('An error occurred while opeaning a HandlerSocket index: :error', array(
				':error' => $this->_hsread->getError()
			));
		}

		$result = $this->_hsread->executeSingle(HS::SELECT, $this->_where[1], $this->_where[2], $limit, $offset);
		
		if ($result === FALSE or ! count($result))
		{
			return FALSE;
		}
		
		return $result;
	}
	
	/**
	 * Formats get() into a friendlier array
	 * 
	 * @param integer Limit number of results
	 * @param integer Skip number of results
	 * @return array|boolean An array of rows or FALSE (if none found).
	 */
	public function get_array($limit = NULL, $offset = NULL)
	{
		$result = $this->get($limit, $offset);
		
		if ($result === FALSE)
		{
			return FALSE;
		}
		
		$result_array = array();
		
		foreach ($result as $row)
		{
			$result_array[] = array_combine($this->_select, $row);
		}
	
		return $result_array;
	}
	
	/**
	 * Formats get() into a friendlier object
	 * 
	 * @param integer Limit number of results
	 * @param integer Skip number of results
	 * @return array|boolean An array of rows or FALSE (if none found).
	 */
	public function get_object($limit = NULL, $offset = NULL)
	{
		$result = $this->get($limit, $offset);
		
		if ($result === FALSE)
		{
			return $result;
		}
		
		$result_object = array();
		
		foreach ($result as $row)
		{
			$result_array[] = (object) array_combine($this->_select, $row);
		}
	
		return $result_object;
	}
	
	/**
	 * Perform update
	 * 
	 * Usage:
	 * 
	 * HS::instance()->select('column')
	 * 		->from('hstable')
	* 		->where('id', '=', array('1'))
	 * 		->update(array('newvalue'));
	 * 
	 * @param array New values
	 * @param integer Limit number of records to update
	 * @param integer Skip number of records before update
	 * @param array Filter records to update
	 * @return integer Number of records updated
	 */
	public function update(array $values = NULL, $limit = 1, $offset = 0, array $filters = NULL)
	{
		if ( ! $this->_hswrite->openIndex(HS::UPDATE, $this->_config['dbname'], $this->_from, $this->_where[0], implode(',', $this->_select)))
		{
			throw new HS_Exception('An error occurred while opeaning a HandlerSocket index: :error', array(
				':error' => $this->_hs->getError()
			));
		}

		$result = $this->_hswrite->executeUpdate(HS::UPDATE, $this->_where[1], $this->_where[2], $values, $limit, $offset);
		
		if ($result === FALSE)
		{
			throw new HS_Exception('There was an error while performing HandlerSocket update. :error', array(
				':error' => $this->_hswrite->getError()
			));
		}

		return $result;
	}
	
	/**
	 * Perform insert
	 * 
	 * Usage:
	 * 
	 * HS::instance()->select('field1', 'field2', 'field3')->from('hstable')->insert(array('value1', 'value2', 'value3'));
	 * or
	 * HS::instance()->select('field1', 'field2', 'field3')->from('hstable')->insert(array(
	 * 	array('value1', 'value2', 'value3'),
	 * 	array('value4', 'value5', 'value6'),
	 * 	...
	 * ));
	 * 
	 * @throws HS_Exception
	 * @params array Values of row to insert
	 * @return boolean
	 * 
	 */
	public functio insert(array $values = NULL)
	{
		if ($values === NULL)
		{
			return FALSE;
		}
	
		if ( ! $this->_hswrite->openIndex(HS::INSERT, $this->_config['dbname'], $this->_from, '', implode(',', $this->_select)))
		{
			throw new HS_Exception('An error occurred while opeaning a HandlerSocket index: :error', array(
				':error' => $this->_hswrite->getError()
			));
		}

		if (isset($values[0]) and is_array($values[0]))
		{
			foreach ($values as $value)
			{
				if ($this->_hswrite->executeInsert(HS::INSERT, $value) === FALSE)
				{
					throw new HS_Exception('An error occurred while performing HandlerSocket insert: :error', array(
						':error' => $this->_hswrite->getError()
					));
				}
			}
			
			return TRUE;
		}
		else
		{
			if ($this->_hswrite->executeInsert(HS::INSERT, $values) === FALSE)
			{
				throw new HS_Exception('An error occurred while performing HandlerSocket insert: :error', array(
					':error' => $this->_hswrite->getError()
				));
			}
			
			return TRUE;
		}
	}

	/**
	 * Perform deletion
	 * 
	 * Usage:
	 * 
	 * HS::instance()->from('hstable')->where('id', '=', array('1'))->delete();
	 * 
	 * @throws HS_Exception
	 * @param integer Limit number of records to be deleted
	 * @param integer Skip number of records before deleting
	 * @return integer|boolean Number of rows deleted or FALSE
	 */
	public function delete($limit = NULL, $offset = NULL, array $filters = NULL)
	{
		if ( ! $this->_hswrite->openIndex(HS::DELETE, $this->_config['dbname'], $this->_from, $this->_where[0], ''))
		{
			throw new HS_Exception('An error occurred while opeaning a HandlerSocket index: :error', array(
				':error' => $this->_hs->getError()
			));
		}

		$result = $this->_hswrite->executeDelete(HS::DELETE, $this->_where[1], $this->_where[2], $limit, $offset, $filters);
				
		if ($result === FALSE)
		{
			throw new HS_Exception('An error occurred while performing HandlerSocket delete: :error', array(
				':error' => $this->_hswrite->getError()
			));
		}

		return $result;	
	}
}
