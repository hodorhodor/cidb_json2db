<?php

/**
 * Command line PHP script to load json_output from cidb scraper into a PostgreSQL database.
 *
 * Tries to do some filtering on bad data, and normalizing it to something we can work with.
 *  
 * Contains rudimentary manual parallel handling:
 * If you want to run this on 2 processes:
 *   On one terminal: php parse.php 0 2
 *   On 2nd terminal: php parse.php 1 2
 * Four processes:
 *   On one terminal: php parse.php 0 4
 *   On 2nd terminal: php parse.php 1 4
 *   On 3rd terminal: php parse.php 2 4
 *   On 4th terminal: php parse.php 3 4
 *
 * Just make sure your disk I/O can keep up. (Hint: mind your checkpoint_segments in postgresql.conf)
 */

$dbname   = 'cidb';
$username = 'cidb';
$password = 'password';

@list(, $this_core, $divisor) = $argv;
if (empty($divisor)) {
    $this_core = 0;
    $divisor   = 1;
}

try {
    $pdo = new PDO("pgsql:host=localhost;dbname=${dbname};user=${username};password=${password}");
} catch (PDOException $e) {
    $msg = $e->getMessage();
    print "$msg\n";
    // Well, originally I wanted to auto create and initialize the database here.
    exit;
}

$dir = 'json_output';
$dh  = opendir($dir);

// Sample columns (so we can build the SQL)
$keys = array(
    'cont_id'   => 0,
    'ctime'     => 0,
    //'json_data' => ''
);

// Mapping columns from json
// Shuddap about code smells and DRY. I know.
$keys0 = array(
    'company'    => 'companyname', // "name" is redundant
    'address'    => 'address',
    'email'      => 'email',
    'telephone'  => 'telephone',
    'fax'        => 'fax',
    'postcode'   => 'postcode',
    'source'     => 'source',
    'status'     => 'status',
    'town'       => 'town',
);

// Yes, there are differences between cidb and pkk
$keys1 = array(
    'cidb_nopendaftaran'          => 'nopendafataran',
    'cidb_bumiputera'             => 'bumiputra', // Note spelling
    'cidb_pengkhususan'           => 'pengkhususan',
    'cidb_tarikhluputpendaftaran' => 'tarikhluputpendaftaran',
    'cidb_vgred'                  => 'vgred',
);

// Yes, there are differences between cidb and pkk
$keys2 = array(
    'pkk_nopendaftaran' => 'nopendafataran',
    'pkk_kelas'         => 'kelas',
);

$keys3 = array(
    'rob'            => 'rob',
    'roc'            => 'roc',
    'tradinglicense' => 'tradinglicense',
);

$keys6 = array(
    'modaldibenarkan' => 'amodaldibenarkanrm',
    'modalberbayar'   => 'bmodalberbayarmodalterkumpulrm',
);

$date_fields = array(
    'cidb_tarikhluputpendaftaran',
);
$numeric_fields = array(
    'modaldibenarkan',
    'modalberbayar',
);

$all_keys = array();
foreach (array_keys($keys) as $v) {
    $all_keys[$v] = ':'.$v;
}
foreach (array_keys($keys0) as $v) {
    $all_keys[$v] = ':'.$v;
}
foreach (array_keys($keys1) as $v) {
    $all_keys[$v] = ':'.$v;
}
foreach (array_keys($keys2) as $v) {
    $all_keys[$v] = ':'.$v;
}
foreach (array_keys($keys3) as $v) {
    $all_keys[$v] = ':'.$v;
}
foreach (array_keys($keys6) as $v) {
    $all_keys[$v] = ':'.$v;
}

$contractor_insert_sql = "insert into contractor (".
    implode(', ', array_keys($all_keys)).
    ") values (".
    implode(', ', array_values($all_keys)).")";

$project_insert_sql  = 'insert into project '.
    '(cont_id, tajuk, tarikh_anugerah, nilai, klien) values '.
    '(:cont_id, :tajuk, :tarikh_anugerah, :nilai, :klien);';
$director_insert_sql = 'insert into director '.
    '(cont_id, nama, jawatan, warganegara) values '.
    '(:cont_id, :nama, :jawatan, :warganegara);';

$contractor_insert = $pdo->prepare($contractor_insert_sql);
$project_insert    = $pdo->prepare($project_insert_sql);
$director_insert   = $pdo->prepare($director_insert_sql);
$contractor_delete = $pdo->prepare("delete from contractor where cont_id = ?");

function fix_date($dmy) {
    $dmy = str_replace(" ", "", $dmy);
    $dmy = str_replace("/", "-", $dmy);

    if (preg_match('/\d+\-\d+\-\d\d\d\d/', $dmy)) {
        list($d, $m, $y) = explode('-', $dmy);
        return $y.'-'.$m.'-'.$d;
    }
    return null;
}
function fix_numeric($rm) {
    $rm = str_replace(",", "", $rm);
    $rm = str_replace(" ", "", $rm);
    if (preg_match('/(\d+)/', $rm, $matches)) {
        return $matches[1];
    }
    return null;
}

function fix_string($str) {
    $str = ltrim(rtrim($str));
    return $str;
}

function fix_email($email) {
    if ($email == '-') {
        return null;
    }
    $lowered_email = strtolower($email);
    if ($lowered_email == 'tiada' || $lowered_email == 'tiada email') {
        return null;
    }
    if (empty($email)) {
        return null;
    }
    return fix_string($email);
}

function fix_phone($phone) {
    if ($phone == '-') {
        return null;
    }
    if (strtolower($phone) == 'tiada') {
        return null;
    }
    return fix_string($phone);
}

while ($file = readdir($dh)) {
    $dirfile = $dir.'/'.$file;

    if (!is_file($dirfile)) {
        continue;
    }

    // Multiprocess support: is this file ours?
    list($num) = explode('.', $file);
    if ($num % $divisor != $this_core) {
        // Nope, not ours.
        continue;
    }

    $stat = stat($dirfile);

    print "Processing $file\n";
    $raw_json = file_get_contents($dirfile);
    $data     = json_decode($raw_json, 1);

    //print_r($data);

    if (empty($data[0]['companyname']) && empty($data[1]['nopendafataran'])) {
        //print_r($data);
        print " Skipping\n";
        continue;
    }

    $cont_id = $data[0]['reference'] * 1; // 1 to lazy normalize string to int
    $row = array(
        ':cont_id'   => $cont_id, 
        ':ctime'     => date('c', $stat['mtime']), // mtime of the file is more accurate as a crawl time. 'c' is for ISO 8601 format
        //':json_data' => $raw_json,
    );

    // DRY, I know, shuddap.
    foreach ($keys0 as $k => $v) {
        // We would rather keep some of the fields as NULL when they are just empty
        if (!empty($data[0][$v])) {
            $row[':'.$k] = $data[0][$v];
        } else {
            $row[':'.$k] = null;
        }
    }
    foreach ($keys1 as $k => $v) {
        // We would rather keep some of the fields as NULL when they are just empty
        if (!empty($data[1][$v])) {
            $row[':'.$k] = $data[1][$v];
        } else {
            $row[':'.$k] = null;
        }
    }
    foreach ($keys2 as $k => $v) {
        // We would rather keep some of the fields as NULL when they are just empty
        if (!empty($data[2][$v])) {
            $row[':'.$k] = $data[2][$v];
        } else {
            $row[':'.$k] = null;
        }
    }
    foreach ($keys3 as $k => $v) {
        // We would rather keep some of the fields as NULL when they are just empty
        if (!empty($data[3][$v])) {
            $row[':'.$k] = $data[3][$v];
        } else {
            $row[':'.$k] = null;
        }
    }

    // This is stupid. Sometimes it's on $data[6], sometimes on $data[7]
    $offset = 5;
    for ($offset = 5; $offset <= 9; $offset++) {
        if (isset($data[$offset]['bmodalberbayarmodalterkumpulrm'])) {
            foreach ($keys6 as $k => $v) {
                // We would rather keep some of the fields as NULL when they are just empty
                if (!empty($data[$offset][$v])) {
                    $row[':'.$k] = $data[$offset][$v];
                } else {
                    $row[':'.$k] = null;
                }
            }
            break;
        }
    }

    // Fixes.
    /*
    if (strtolower($row[':cidb_bumiputera']) == 'tiada maklumat') {
        $row[':cidb_bumiputera'] = null;
    }*/
    $row[':email'] = fix_email($row[':email']);

    foreach ($date_fields as $v) {
        if (!isset($row[':'.$v])) {
            continue;
        }
        $row[':'.$v] = fix_date($row[':'.$v]);
    }
    foreach ($numeric_fields as $v) {
        if (!isset($row[':'.$v])) {
            continue;
        }
        $row[':'.$v] = fix_numeric($row[':'.$v]);
    }

    // Poor man's INSERT or UPDATE:
    $contractor_delete->execute(array($cont_id));
    $retcode = $contractor_insert->execute($row);
    if (!$retcode) {
        print_r($contractor_insert->errorInfo());
        print_r($row);
        die("Error inserting contractor\n");
    }

    // Directors
    for ($offset = 5; $offset <= 9; $offset++) {
        if (isset($data[$offset]) &&
            is_array($data[$offset]) &&
            isset($data[$offset][0]) &&
            isset($data[$offset][0]['jawatan'])) {
            foreach ($data[$offset] as $director) {
                if (!empty($director['nama'])) {
                    //print_r($director);
                    $row2 = array(
                        ':cont_id'     => $cont_id,
                        ':nama'        => $director['nama'],
                        ':jawatan'     => $director['jawatan'],
                        ':warganegara' => $director['warganegara'],
                    );
                    $retcode = $director_insert->execute($row2);
                    if (!$retcode) {
                        print_r($director_insert->errorInfo());
                        print_r($row2);
                        die("Error inserting director\n");
                    }
                }
            }
            break;
        }
    }

    // Projects
    for ($offset = 5; $offset <= 9; $offset++) {
        if (isset($data[$offset]) &&
            is_array($data[$offset]) &&
            isset($data[$offset][0]) &&
            isset($data[$offset][0]['tajuk'])) {
            foreach ($data[$offset] as $project) {
                if (!empty($project['tajuk'])) {
                    //print_r($project);
                    $row2 = array(
                        ':cont_id'         => $cont_id,
                        ':tajuk'           => fix_string($project['tajuk']),
                        ':tarikh_anugerah' => fix_date($project['tarikhanugerah']),
                        ':nilai'           => fix_numeric($project['nilairm']),
                        ':klien'           => $project['kilen'], // SIC. Note wrong spellin on "kilen"
                    );
                    //print_r($row2);
                    $retcode = $project_insert->execute($row2);
                    if (!$retcode) {
                        print_r($project_insert->errorInfo());
                        print_r($row2);
                        die("Error inserting project\n");
                    }
                }
            }
            break;
        }
    }
}
