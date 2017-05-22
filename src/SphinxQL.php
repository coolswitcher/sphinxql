<?php
/**
 * Mysqli interface to SphinxQL
 *
 * How to use:
 * $db = \AppZz\DB\SphinxQL::factory ()
 * 				->select('*')
 * 				->from('index')
 * 				->match('@field', 'foobar')
 * 				->where('field2', '>', 2)
 * 				->between('field3', 10, 90)
 * 				->execute();
 */
namespace AppZz\DB;

/**
 * @package SphinxQL
 * @version 1.0.0
 * @author CoolSwitcher
 * @license MIT
 */
class SphinxQL {

	private $sphinxql    = NULL;
	private $debug       = FALSE;
	private $opts        = array ();
	private $where       = array ();
	private $stats       = array ();

	private $default_opts = array (
		'limit'       =>20,
		'offset'      =>0,
		'pairs'       => FALSE,
		'max_matches' =>1000,
	);

	public function __construct ( $host = '127.0.0.1', $port = 9306, $debug = FALSE ) {
	    $this->sphinxql = mysqli_init();
	    if ( !$this->sphinxql )
	    	$this->error ('MySQLi not initialed');
		if ( !@mysqli_real_connect ( $this->sphinxql, $host, NULL, NULL, NULL, $port, NULL ) )
			$this->error ('Can\'t connect to SphinxQL: ' . mysqli_connect_errno() . ' ' . mysqli_connect_error() );
		$this->opts  = $this->default_opts;
		$this->debug = (bool) $debug;
	}

	public static function factory ( $host = '127.0.0.1', $port = 9306, $debug = FALSE ) {
		return new SphinxQL ( $host, $port, $debug );
	}

	public function select ( $fields = '*' ) {
		if ( empty ($fields) )
			$this->error ('Fields are empty');
		$this->opts['fields'] = $fields;
		if ( strpos ($fields, ',') AND $fields != '*' ) {
			$fields_array = explode (',', $fields);
			$fields_array = array_map ('trim', $fields_array);
			$this->opts['fields_array'] = $fields_array;
		}
		return $this;
	}

	public function from ( $index = 'index' ) {
		if ( empty ($index) )
			$this->error ('Index are empty');		
		$this->opts['index'] = $index;
		return $this;
	}

	public function match ($field, $values, $exact_match = FALSE) {
		$this->_where ($field, $values, 'match', $exact_match);
		return $this;
	}

	public function where ($field, $cmp, $values) {
		$this->_where ($field, $values, 'default', $cmp);
		return $this;
	}

	public function between ($field, $min, $max) {
		if ( $min >= $max )
			$this->error('min may be smalller than max');
		$this->_where ($field, array ($min, $max), 'between');
		return $this;
	}

	public function groupby ($field) {
		$this->opts['groupby'] = $field;
		return $this;
	}

	public function orderby ($field, $asc = 'asc') {
		$this->opts['orderby'][$field] = strtoupper($asc);
		return $this;
	}	

	public function limit ($offset = 0, $limit = 0) {
 		if ( $offset !== 0 AND $limit === 0 ) {
			$this->opts['offset'] = 0;
			$this->opts['limit']  = $offset;			
 		}
		else {
			$this->opts['offset'] = $offset;
			$this->opts['limit']  = $limit;			
		} 
		return $this;
	}

	public function max_matches ($max = 1000) {
		$this->opts['max_matches'] = intval ($max);
		return $this;
	}	

	public function pairs ($pairs = TRUE) {
		$this->opts['pairs'] = (bool) $pairs;
		return $this;
	}		

	public function has_query () {
		return (bool) !empty ($this->where);
	}

	public function rawQuery ( $query ) {
		if ($this->debug)
			$start = microtime(TRUE);

		$result = $this->sphinxql->query ($query);

		if ($this->debug) {
			$timer = microtime(TRUE) - $start;			
			$this->stats[] = array(
				'query' => $query,
				'start' => $start,
				'timer' => $timer,
			);					
		}	

		if ( $result !== FALSE ) {
			$return = array ();
			if ( $result->num_rows > 0 ) {
		        while ( $row = $result->fetch_assoc() ) {	 
		        	$return[] = $row;	        		
		        }				
			}
	        $result->free();
	        if ($this->debug)
	        	$this->cutStats();
	        return $return;	        			
		} else {
			$error = $this->sphinxql->error;	
			if ($this->debug) {
				end($this->stats);
				$key = key($this->stats);
				$this->stats[$key]['error'] = $error;
				$this->cutStats();							
			}		
			$this->error("{$error}. Full query: [{$query}]");	
		}
		return FALSE;				
	}

	public function execute () {
		$where = $this->_buildWhere();
		$order = $this->_buildGroupOrder();
		if ( empty ($where) )
			$this->error ('Where conditions are empty');
		$query = sprintf ("SELECT %s, WEIGHT() as weight FROM %s WHERE %s%s LIMIT %d, %d option max_matches=%d", $this->opts['fields'], $this->opts['index'], $where, $order, $this->opts['offset'], $this->opts['limit'], $this->opts['max_matches']);
		
		if ($this->debug)
			$start = microtime(TRUE);

		$result = $this->sphinxql->query ($query);

		if ($this->debug) {
			$timer = microtime(TRUE) - $start;			
			$this->stats[] = array(
				'query' => $query,
				'start' => $start,
				'timer' => $timer,
			);					
		}		

	    if ( $result !== FALSE ) {
	    	$return = array ();
	    	if ( !isset ($this->opts['fields_array']) AND ($this->opts['fields'] != '*') )
	    		$sp_result = 'column';
        	elseif ( sizeof ($this->opts['fields_array']) === 2 AND $this->opts['pairs'] !== FALSE ) {
				$sp_result = 'pairs';
				$sp_key = array_shift($this->opts['fields_array']);
				$sp_value = array_pop($this->opts['fields_array']);	
			} else {
				$sp_result = 'row';
			}

	        while ( $row = $result->fetch_assoc() ) {	 
	        	switch ($sp_result) {
	        		case 'column':
	        			$return[] = $row[$this->opts['fields']];
	        		break;
	        		case 'pairs':	
	        			$return[$row[$sp_key]] = $row[$sp_value];
	        		break;
	        		default:	
	        			$return[] = $row;	        		
	        		break;	
	        	}           	
	        }
	        $result->free();
	        if ($this->debug)
	        	$this->cutStats();
	       	$this->flushOpts();
	        return $return;
	    } else {
			$error = $this->sphinxql->error;	
			if ($this->debug) {
				end($this->stats);
				$key = key($this->stats);
				$this->stats[$key]['error'] = $error;
				$this->cutStats();							
			}		
			$this->error("{$error}. Full query: [{$query}]");	
	    }
	   	$this->flushOpts();
	    return false;		
	}

	public function getStats() {
		return $this->stats;
	}	

	public function lastQuery() {
		$last = end($this->stats);
		return isset ($last['query']) ? $last['query'] : FALSE;
	}	

	private function flushOpts () {
		$this->opts  = $this->default_opts;
		$this->where = array ();
	}

	private function _where ($field, $values, $type, $extra = NULL) {
		$field = $this->_checkField ($field);

		if ( is_array ($values) AND (!empty ($values)) AND ($extra === 'in') ) {
			$type = 'in';
			$extra = NULL;
		}

		$this->where[] = array ('field'=>$field, 'values'=>$values, 'type'=>$type, 'prefix'=>$this->_where_prefix(), 'extra'=>$extra);
	}		

	private function _where_prefix () {
		return !empty ( $this->where ) ? 'AND' : '';
	}

	public function _escape_string ( $string ) {
	    $from = array ( '\\', '(',')','|','-','!','@','~','"','&', '/', '^', '$', '=', "'", "\x00", "\n", "\r", "\x1a" );
	    $to   = array ( '\\\\', '\\\(','\\\)','\\\|','\\\-','\\\!','\\\@','\\\~','\\\"', '\\\&', '\\\/', '\\\^', '\\\$', '\\\=', "\\'", "\\x00", "\\n", "\\r", "\\x1a" );
	    return str_replace ( $from, $to, $string );
	}	

	private function _escape ( $value ) {
		$value = trim ($value);
		if ( !is_numeric($value) && !is_null ($value) ) {
			$value = "'" . $this->_escape_string ($value) . "'";
		}
		return $value;
	}

	private function _escapeMatch ( $value, $exact_match = FALSE ) {
		$value = (array) $value;
		$obj = $this;
		$value = array_map (function ($v) use ($exact_match, $obj) {
			$v = trim ($v);
			$v = $obj->_escape_string ($v);
			if ($exact_match === -1)
				$v = '"^' . $v . '"';
			else
				$v = $exact_match ? '"^' . $v . '$"' : '"' . $v . '"';
			return $v;
		}, $value);
		return implode (' | ', $value);
	}

	private function _escapeValues ( $values ) {
		if ( !is_array ($values) ) 
			$values = array ($values);
		$values = array_map( array ($this, '_escape'), $values);	 
		return $values;
	}	

	private function _checkField ( $field ) {
		$field = preg_replace ('#[^a-z_,@\(\)]+#iu', '', $field);
		if ( empty ($field) )
			$this->error ('Wrong field name');
		return trim ($field);		
	}	

	private function _buildWhere () {
		$where = '';
		$matches = '';
		if ( !empty ($this->where) ) {
			foreach ($this->where as &$param) {
				if ($param['type'] === 'match')
					$values = $this->_escapeMatch($param['values'], $param['extra']);
				else
					$values = $this->_escapeValues($param['values']);
				switch ( $param['type'] ) {
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
			if ( !empty ($matches) ) {
				$matches = 'MATCH (\'' . trim ($matches) . '\')';
				if ( !empty ($where) )
					$where = $matches . ' ' . $where;
				else
					$where = $matches; 
			}
		}
		return trim ($where);
	}

	private function _buildGroupOrder () {
		$group_order = '';
		if ( isset ($this->opts['groupby']) AND !empty ($this->opts['groupby']) ) 
			$group_order = 'GROUP BY ' . $this->opts['groupby'];
		if ( isset ($this->opts['orderby']) AND !empty ($this->opts['orderby']) ) {
			$tot_orders = sizeof($this->opts['orderby']);
			$ord_cnt    = 0; 
			$group_order .= ' ORDER BY ';
			foreach ($this->opts['orderby'] as $field=>&$asc) {
				$ord_cnt++;
				$group_order .= $field . ' ' . $asc;
				if ( $ord_cnt < $tot_orders )
					$group_order .= ', '; 
			}
		}
		return ' ' . trim ($group_order);
	}

	private function cutStats() {
		if ( sizeof($this->stats) > 100 ) {
			reset($this->stats);
			$first = key($this->stats);
			unset($this->stats[$first]);
		}
	}	

	private function error ( $err ) {
		throw new \Exception ($err);
	}

	public function close () {
		$this->sphinxql->close();
	}

	public function __destroy () {
		$this->close();
	}

}