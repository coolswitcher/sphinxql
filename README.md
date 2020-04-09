# SphinxQL
Mysqli interface for SphinxQL
```
//Select query
$db = \AppZz\DB\SphinxQL::factory ('127.0.0.1', 9323)
            ->select('index', ['id', 'title'])
            ->match('@field', 'foobar')
            ->where('field2', '>', 2)
            ->between('field3', 10, 90);
          
$result = $db->execute();
          
if ($result) {
    $data = $db->as_array();
    var_dump($data);
}

//Update RT index
$update = \AppZz\DB\SphinxQL::factory ('127.0.0.1', 9323)
            ->replace('index', ['id', 'title'])
            ->values([11, 'new title'])
            ->execute();          

//affected rows
var_dump($update);
```  				
