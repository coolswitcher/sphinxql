<?php
/**
 * Mysqli interface to SphinxQL
 *
 * How to use:
 * $db = \AppZz\DB\SphinxQL::factory ()
 * 				->select('index', ['id', 'title'])
 * 				->match('@field', 'foobar')
 * 				->where('field2', '>', 2)
 * 				->between('field3', 10, 90)
 * 				->execute()
 * 				->as_array();
 */
namespace AppZz\DB;

/**
 * @package SphinxQL
 * @version 1.2.0
 * @author CoolSwitcher
 * @license MIT
 */
class SphinxQL {

	private $_db    = NULL;
	private $_debug = FALSE;
	private $_opts  = array ();
	private $_where = array ();
	private $_stats = array ();
	private $_query = 'SELECT';
	private $_index;
	private $_fields;
	private $_fields_array;
	private $_values;
	private $_result;

	const SELECT    = 'SELECT';
	const UPDATE    = 'UPDATE';
	const INSERT    = 'INSERT';
	const REPLACE   = 'REPLACE';
	const DELETE    = 'DELETE';

	private $_default_opts = array (
		'limit'       => 20,
		'offset'      => 0,
		'max_matches' => 1000,
	);

	public function __construct ($host = '127.0.0.1', $port = 9306, $debug = FALSE)
	{
	    $this->_db = mysqli_init();

	    if ( ! $this->_db ) {
	    	$this->_error ('MySQLi not initialed');
	    }

		if ( ! mysqli_real_connect ( $this->_db, $host, NULL, NULL, NULL, $port, NULL ) ) {
			$this->_error ('Can\'t connect to SphinxQL: ' . mysqli_connect_errno() . ' ' . mysqli_connect_error() );
		}

		$this->_opts   = $this->_default_opts;
		$this->_debug = (bool) $debug;
	}

	public static function factory ($host = '127.0.0.1', $port = 9306, $debug = FALSE)
	{
		return new SphinxQL ($host, $port, $debug);
	}

	public function fields ($fields = '*')
	{
		if ( ! empty ($fields)) {
			if (is_array($fields)) {
				$this->_fields = implode (', ', $fields);
			} else {
				$this->_fields = $fields;
			}
		}
	}

	public function select ($index = 'index', $fields = '*')
	{
		$this->_query = SphinxQL::SELECT;
		$this->fields($fields);
		$this->index($index);
		return $this;
	}

	public function insert ($index = 'index', array $fields = array ())
	{
		$this->_query = SphinxQL::INSERT;
		$this->fields($fields);
		$this->index($index);
		return $this;
	}

	public function replace ($index = 'index', array $fields = array ())
	{
		$this->_query = SphinxQL::REPLACE;
		$this->fields($fields);
		$this->index($index);
		return $this;
	}

	public function delete ($index = 'index')
	{
		$this->_query = SphinxQL::DELETE;
		$this->index($index);
		return $this;
	}

	public function index ($index = 'index')
	{
		if (empty ($index)) {
			$this->_error ('Index are empty');
		}

		$this->_index = $index;
		return $this;
	}

	public function values (array $values = array())
	{
		if (empty ($values)) {
			$this->_error ('Values are empty');
		}

		$this->_values = $values;
		return $this;
	}

	public function match ($field, $values, $exact_match = FALSE)
	{
		$this->_where ($field, $values, 'match', $exact_match);
		return $this;
	}

	public function where ($field, $cmp, $values)
	{
		$this->_where ($field, $values, 'default', $cmp);
		return $this;
	}

	public function between ($field, $min, $max)
	{
		if ($min >= $max) {
			$this->_error('min may be smalller than max');
		}

		$this->_where ($field, array ($min, $max), 'between');
		return $this;
	}

	public function groupby ($field)
	{
		$this->_opts['groupby'] = $field;
		return $this;
	}

	public function orderby ($field, $asc = 'asc')
	{
		$asc = strtoupper ($asc);
		$this->_opts['orderby'] = $field;
		$this->_opts['order']   = in_array($asc, array('ASC','DESC')) ? $asc : 'ASC';
		return $this;
	}

	public function limit ($offset = 0, $limit = 0)
	{
 		if ($offset !== 0 AND $limit === 0) {
			$this->_opts['offset'] = 0;
			$this->_opts['limit']  = $offset;
 		}
		else {
			$this->_opts['offset'] = $offset;
			$this->_opts['limit']  = $limit;
		}

		return $this;
	}

	public function max_matches ($max = 1000)
	{
		$this->_opts['max_matches'] = intval ($max);
		return $this;
	}

	public function execute ()
	{
		$where = $this->_build_where();
		$order = $this->_build_group_order();

		if ($this->_query === SphinxQL::SELECT AND $this->_query !== SphinxQL::DELETE) {
			if (empty ($where)) {
				$this->_error ('Where conditions are empty');
			}
		}

		switch ($this->_query) {
			case SphinxQL::SELECT:
					if (empty ($this->_fields)) {
						$this->_error ('Fields are empty');
					}

					$query = sprintf ("SELECT %s, WEIGHT() as weight FROM %s WHERE %s%s LIMIT %d, %d option max_matches=%d", $this->_fields, $this->_index, $where, $order, $this->_opts['offset'], $this->_opts['limit'], $this->_opts['max_matches']);
				break;

			case SphinxQL::INSERT:
			case SphinxQL::REPLACE:
				$values = $this->_build_values();
				$query  = sprintf ("%s INTO %s %s", $this->_query, $this->_index, $values);
				break;

			case SphinxQL::DELETE:
				$query = sprintf ("DELETE FROM %s WHERE %s", $this->_index, $where);
				break;
		}

		if ($this->_debug) {
			$start = microtime(TRUE);
		}

		$this->_result = $this->_db->query ($query);

		if ($this->_debug) {
			$timer = microtime(TRUE) - $start;
			$this->_stats[] = array(
				'query' => $query,
				'start' => $start,
				'timer' => $timer,
			);
		}

		if ($this->_result !== FALSE) {
			if ($this->_query !== SphinxQL::SELECT) {
				return $this->_db->affected_rows;
			} else {
				return ($this->_result !== FALSE);
			}
		}

		return FALSE;
	}

	public function as_array ($id = NULL, $name = NULL)
	{
	    $return = array ();

	    if ($this->_result !== FALSE AND $this->_query === SphinxQL::SELECT) {
	        while ($row = $this->_result->fetch_assoc()) {
	        	if (empty($id) AND empty($name)) {
	        		$return[] = $row;
	        	} else {
	        		if ( ! empty ($name)) {
	        			if (empty($id)) {
	        				$return[] = $row[$name];
	        			} else {
	        				$return[$row[$id]] = $row[$name];
	        			}
	        		}
	        	}
	        }

		    $this->_result->free();

	        if ($this->_debug) {
	        	$this->_cut_stats();
	        }

	       	$this->_flush_opts();
	    }

	    return $return;
	}

	public function get_stats()
	{
		return $this->_stats;
	}

	public function last_query ()
	{
		$last = end($this->_stats);
		return isset ($last['query']) ? $last['query'] : FALSE;
	}

	private function _flush_opts ()
	{
		$this->_opts = $this->_default_opts;
		$this->where = array ();
	}

	private function _where ($field, $values, $type, $extra = NULL)
	{
		$field = $this->_check_field ($field);

		if (is_array ($values) AND (!empty ($values)) AND ($extra === 'in')) {
			$type = 'in';
			$extra = NULL;
		}

		$this->where[] = array ('field'=>$field, 'values'=>$values, 'type'=>$type, 'prefix'=>$this->_where_prefix(), 'extra'=>$extra);
	}

	private function _where_prefix ()
	{
		return !empty ( $this->where ) ? 'AND' : '';
	}

	public function _escape_string ($string)
	{
	    $from = array ( '\\', '(',')','|','-','!','@','~','"','&', '/', '^', '$', '=', "'", "\x00", "\n", "\r", "\x1a" );
	    $to   = array ( '\\\\', '\\\(','\\\)','\\\|','\\\-','\\\!','\\\@','\\\~','\\\"', '\\\&', '\\\/', '\\\^', '\\\$', '\\\=', "\\'", "\\x00", "\\n", "\\r", "\\x1a" );
	    return str_replace ($from, $to, $string);
	}

	private function _escape ($value)
	{
		$value = trim ($value);

		if ( ! is_numeric($value) AND ! is_null ($value)) {
			$value = "'" . $this->_escape_string ($value) . "'";
		}

		return $value;
	}

	private function _escape_match ($value, $exact_match = FALSE)
	{
		$value = (array) $value;
		$obj = $this;

		$value = array_map (function ($v) use ($exact_match, $obj) {
			$v = trim ($v);
			$v = $obj->_escape_string ($v);
			if ($exact_match === -1) {
				$v = '"^' . $v . '"';
			} else {
				$v = $exact_match ? '"^' . $v . '$"' : '"' . $v . '"';
			}

			return $v;
		}, $value);

		return implode (' | ', $value);
	}

	private function _escape_values ($values)
	{
		if ( ! is_array ($values)) {
			$values = array ($values);
		}

		$values = array_map (array ($this, '_escape'), $values);
		return $values;
	}

	private function _check_field ($field)
	{
		$field = preg_replace ('#[^a-z_,@\(\)]+#iu', '', $field);

		if (empty ($field)) {
			$this->_error ('Wrong field name');
		}

		return trim ($field);
	}

	private function _build_values ()
	{
		$values = $this->_escape_values ($this->_values);
		return sprintf ("(%s) VALUES (%s)", $this->_fields, implode (', ', array_values($values)));
	}

	private function _build_where ()
	{
		$where = '';
		$matches = '';

		if ( ! empty ($this->where)) {
			foreach ($this->where as &$param) {
				if ($param['type'] === 'match') {
					$values = $this->_escape_match($param['values'], $param['extra']);
				} else {
					$values = $this->_escape_values($param['values']);
				}

				switch ($param['type']) {
					case 'between':
						  $where .= ( !empty ($param['prefix']) ? $param['prefix'] . ' ' : ' ' ) . $param['field'] . ' BETWEEN ' . $values[0] . ' AND ' . $values[1] . ' ';
					break;
					case 'match':
						$matches .= $param['field'] . ' ' . $values . ' ';
					break;
					case 'in':
						$where .= ( !empty ($param['prefix']) ? $param['prefix'] . ' ' : '' ) . $param['field'] . ' IN (' . implode (', ', $values) . ') ';
					break;
					default:
						  $where .= ( !empty ($param['prefix']) ? $param['prefix'] . ' ' : '' ) . $param['field'] . ' ' . $param['extra'] . ' ' . $values[0] . ' ';
					break;
				}
			}

			if ( ! empty ($matches) ) {
				$matches = 'MATCH (\'' . trim ($matches) . '\')';

				if ( ! empty ($where)) {
					$where = $matches . ' ' . $where;
				} else {
					$where = $matches;
				}
			}
		}

		return trim ($where);
	}

	private function _build_group_order ()
	{
		$group_order = '';

		if ( ! empty ($this->_opts['groupby']))
		{
			$group_order = sprintf (' GROUP BY %s', $this->_opts['groupby']);
		}

		if ( ! empty ($this->_opts['orderby']) AND ! empty ($this->_opts['order']))
		{
			$group_order .= sprintf (' ORDER BY %s %s', $this->_opts['orderby'], $this->_opts['order']);
		}

		return ' ' . trim ($group_order);
	}

	private function _cut_stats ()
	{
		if (sizeof($this->_stats) > 100) {
			reset($this->_stats);
			$first = key($this->_stats);
			unset($this->_stats[$first]);
		}
	}

	private function _error ($message)
	{
		throw new \Exception ($message);
	}

	public function close ()
	{
		$this->_db->close();
	}

	public function __destroy ()
	{
		$this->close();
	}
}
