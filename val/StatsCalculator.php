<?php

namespace App\Utils\MLB;

use App\Utils\LeaguesImporter\Dto\LeagueData;
use Illuminate\Database\Eloquent\Model;
use Illuminate\Support\Collection;

/**
 * Used for calculation different baseball statistic such as AVG, OPS, OBP, SLG etc.
 */
class StatsCalculator
{   
    /**
     * Calls specific method to calculate the desired stats.
     *
     * @param string $method
     * @param array $args
     * @return int|float
     */
    public static function __callStatic($method, array $args)
    {
        if (!$args)
        {
            throw new \BadMethodCallException('Stats record is not given.');
        }
        $s = $args[0];
        switch ($method)
        {
            case 'tb':
                return static::tb(self::normalize($s, ['h', '2b', '3b', 'hr']));
            case 'avg':
                return static::avg(self::normalize($s, ['h', 'ab']));
            case 'obp':
                return static::obp(self::normalize($s, ['ab', 'bb', 'hbp', 'sf', 'h']));
            case 'slg':
                return static::slg(self::normalize($s, ['ab', 'h', '2b', '3b', 'hr']));
            case 'ops':
                return static::ops(self::normalize($s, ['ab', 'bb', 'hbp', 'sf', 'h', '2b', '3b', 'hr']));
            case 'era':
                return static::era(self::normalize($s, ['er', 'ip']));
            case 'sbp':
                return static::sbp(self::normalize($s, ['sb', 'cs']));
            case 'qsp':
                return static::qsp(self::normalize($s, ['qs', 'gs']));
            case 'wlp':
                return static::wlp(self::normalize($s, ['w', 'l']));
            case 'svp':
                return static::svp(self::normalize($s, ['sv', 'svopp']));
            case 'irsp':
                return static::irsp(self::normalize($s, ['irs', 'ir']));
            case 'oavg':
                return static::oavg(self::normalize($s, ['h', 'tbf', 'bb', 'hbp', 'sf', 'sh']));
            case 'oobp':
                return static::oobp(self::normalize($s, ['h', 'tbf', 'sh']));
            case 'oslg':
                return static::oobp(self::normalize($s, ['h', '2b', '3b', 'hr', 'tbf', 'bb', 'hbp', 'sf', 'sh']));
            case 'kbb':
                return static::oobp(self::normalize($s, ['k', 'bb']));
            case 'h9':
                return static::h9(self::normalize($s, ['h', 'ip']));
            case 'k9':
                return static::k9(self::normalize($s, ['k', 'ip']));
            case 'bb9':
                return static::bb9(self::normalize($s, ['bb', 'ip']));
            case 'hr9':
                return static::hr9(self::normalize($s, ['hr', 'ip']));
            case 'br9':
                return static::br9(self::normalize($s, ['h', 'bb', 'hbp', 'ip']));
            case 'rs9':
                return static::rs9(self::normalize($s, ['h', 'ip']));
            case 'opsplus':
                return static::opsplus(self::normalize($s, ['opsplus_pa', 'pa']));
            case 'woba':
                return static::woba(self::normalize($s, ['woba_pa', 'pa']));
            case 'wrcplus':
                return static::wrcplus(self::normalize($s, ['wrcplus_pa', 'pa']));
            case 'erc':
                return static::erc(self::normalize($s, ['h', 'bb', 'hbp', 'hr', 'ibb', 'tbf', 'ip']));
            case 'csp':
                return static::csp(self::normalize($s, ['cs', 'sb']));
            case 'ccsp':
                return static::ccsp(self::normalize($s, ['cs', 'sb', 'pcs']));
        }
        throw new \BadMethodCallException('Method ' . $method . ' does not exist.');
    }
    
    /**
     * Types for stats field.
     *
     * @var array
     */
    public static $filedTypes = [
        'int' => ['g','r','er', 'gf', 'ir', 'k', 'h', 'hr', 'bb', 'irs', 'hld', 'l', 'blsv', 'opp', '2b', '3b', 'rbi',
            'pa', 'ab', 'rp', 'sb', 'cs', 'ts', 'gs', 'qs', 'po', 'tc', 'a', 'e', 'dp', 'pb', 'sba', 'w', 'dl', 'ibb', 'pcs',
            'ws', 'woba_pa', 'opsplus_pa', 'wrcplus_pa'],
    ];
    
    /**
     * List of fields that must be removed from scaled stats.
     *
     * @var array
     */
    public static $fieldsRemovedFromScaleStats = ['fip', 'xfip', 'eraplus', 'eraminus', 'siera',
        'gmli', 'opsplus', 'woba', 'wrcplus', 'wpa', 'whip',
        'k9', 'bb9', 'br9', 'hr9', 'rs9'
    ];

    /**
     * Return the array of contained fields with type.
     *
     * @return array
     */
    public static function getFieldsType(): array
    {
        $filedTypes = [];
        foreach (static::$filedTypes as $type => $fields) {
            $filedTypes = array_merge($filedTypes, array_fill_keys($fields, $type));
        }
        
        return $filedTypes;
    }
    
    /**
     * Return the array of contained formulas in which the key it is a fields from database associated with formulas
     * in this class.
     *
     * @return array
     */
    public static function getFieldsMap()
    {
        return [
            'qsp' => 'qsp',
            'wlp' => 'wlp',
            'era' => 'era',
            'svpct' => 'svp',
            'irsp' => 'irsp',
            'sbpct' => 'sbp',
            'avg' => 'avg',
            'obp' => 'obp',
            'slg' => 'slg',
            'ops' => 'ops',
            'rs9' => 'rs9',
            'fav' => 'fav',
            'a9' => 'a9',
            'tc9' => 'tc9',
            'oavg' => 'oavg',
            'oobp' => 'oobp',
            'oslg' => 'oslg',
            'kbb' => 'kbb',
            'h9' => 'h9',
            'k9' => 'k9',
            'bb9' => 'bb9',
            'hr9' => 'hr9',
            'br9' => 'br9',
            'fip' => 'fip',
            'xfip' => 'xfip',
            'opsplus' => 'opsplus',
            'woba' => 'woba',
            'wrcplus' => 'wrcplus',
            'eraplus' => 'eraplus',
            'eraminus' => 'eraminus',
            'siera' => 'siera',
            'gmli' => 'gmli',
            'whip' => 'whip',
            'erc' => 'erc'
        ];
    }

    /**
     * Formula: if (9 * ((h + bb + hbp) * (0.89 * (1.255 * (h - hr) + 4 * hr) + .56 * (bb + hbp - ibb))) / (tbf * ip)) < 2.24
     *   then: (9 * ((h + bb + hbp) * (0.89 * (1.255 * (h - hr) + 4 * hr) + .56 * (bb + hbp - ibb))) / (tbf * ip)) * .75
     *   else: (9 * ((h + bb + hbp) * (0.89 * (1.255 * (h - hr) + 4 * hr) + .56 * (bb + hbp - ibb))) / (tbf * ip)) - .56
     * Pitcher: erc
     *
     * @param array $s
     * @return int
     */
    protected static function erc(array $s)
    {
        $d = $s['ip'];
        $tbf = $s['tbf'];
        if ($d * $tbf != 0)
        {
            if ((9 * (($s['h'] + $s['bb'] + $s['hbp']) * (0.89 * (1.255 * ($s['h'] - $s['hr']) + 4 * $s['hr']) + .56 * ($s['bb'] + $s['hbp'] - $s['ibb']))) / ($s['tbf'] * $s['ip'])) < 2.24){
                return (9 * (($s['h'] + $s['bb'] + $s['hbp']) * (0.89 * (1.255 * ($s['h'] - $s['hr']) + 4 * $s['hr']) + .56 * ($s['bb'] + $s['hbp'] - $s['ibb']))) / ($s['tbf'] * $s['ip'])) * .75;
            } else {
                return (9 * (($s['h'] + $s['bb'] + $s['hbp']) * (0.89 * (1.255 * ($s['h'] - $s['hr']) + 4 * $s['hr']) + .56 * ($s['bb'] + $s['hbp'] - $s['ibb']))) / ($s['tbf'] * $s['ip'])) - .56;
            }
        }
        return 0;
    }
    
    /**
     * Formula: CS/(CS+SB)
     * Catcher: CSP scaled table
     *
     * @param array $s
     * @return int
     */
    protected static function csp(array $s)
    {
        if ($s['cs'] + $s['sb'] != 0)
        {
            return 100 * $s['cs']/($s['cs'] + $s['sb']);
        }
        return 0;
    }
    
    /**
     * Formula:  (CS-PCS)/(CS+SB-PCS)
     * Catcher: CCSP scaled table
     *
     * @param array $s
     * @return int
     */
    protected static function ccsp(array $s)
    {
        if ($s['cs'] + $s['sb'] - $s['pcs']!= 0)
        {
            return 100*($s['cs']- $s['pcs'])/($s['cs'] + $s['sb'] - $s['pcs']);
        }
        return 0;
    }

    /**
     * Formula: (bb + h) / ip
     * Pitcher: WHIP
     *
     * @param array $s
     * @return int
     */
    protected static function whip(array $s)
    {
        $d = $s['ip'];
        if ($d > 0)
        {
            return ($s['bb'] + $s['h']) / $d;
        }
        return 0;
    }

    /**
     * Formula: gmli_g / g
     * Pitcher: Career gmLI
     *
     * @param array $s
     * @return int
     */
    protected static function gmli(array $s)
    {
        $d = $s['g'];
        if ($d > 0)
        {
            return $s['gmli_g'] / $d;
        }
        return 0;
    }

    /**
     * Formula: siera_pa / pa
     * Pitcher: Career SIERA
     *
     * @param array $s
     * @return int
     */
    protected static function siera(array $s)
    {
        $d = $s['ip'];
        if ($d > 0)
        {
            return $s['siera_outs'] / $d;
        }
        return 0;
    }

    /**
     * Formula: eraminus_pa / pa
     * Pitcher: Career ERA-
     *
     * @param array $s
     * @return int
     */
    protected static function eraminus(array $s)
    {
        $d = $s['ip'];
        if ($d > 0)
        {
            return $s['eraminus_outs'] / $d;
        }
        return 0;
    }

    /**
     * Formula: eraplus_pa / pa
     * Pitcher: Career ERA+
     *
     * @param array $s
     * @return int
     */
    protected static function eraplus(array $s)
    {
        $d = $s['ip'];
        if ($d > 0)
        {
            return $s['eraplus_outs'] / $d;
        }
        return 0;
    }

    /**
     * Formula: wrcplus_pa / pa
     * Batter: Career wRC+
     *
     * @param array $s
     * @return int
     */
    protected static function wrcplus(array $s)
    {
        $d = $s['pa'];
        if ($d > 0)
        {
            return $s['wrcplus_pa'] / $d;
        }
        return 0;
    }

    /**
     * Formula: woba_pa / pa
     * Batter: Career wOBA
     *
     * @param array $s
     * @return int
     */
    protected static function woba(array $s)
    {
        $d = $s['pa'];
        if ($d > 0)
        {
            return $s['woba_pa'] / $d;
        }
        return 0;
    }

    /**
     * Formula: opsplus_pa / pa
     * Batter: Career OPS+
     *
     * @param array $s
     * @return int
     */
    protected static function opsplus(array $s)
    {
        $d = $s['pa'];
        if ($d > 0)
        {
            return $s['opsplus_pa'] / $d;
        }
        return 0;
    }

    /**
     * Formula: fip_outs / ip
     * Batter: Career FIP
     *
     * @param array $s
     * @return int
     */
    protected static function fip(array $s)
    {
        $d = $s['ip'];
        if ($d > 0)
        {
            return $s['fip_outs'] / $d;
        }
        return 0;
    }

    /**
     * Formula: xfip_outs / ip
     * Batter: Career xFIP
     *
     * @param array $s
     * @return int
     */
    protected static function xfip(array $s)
    {
        $d = $s['ip'];
        if ($d > 0)
        {
            return $s['xfip_outs'] / $d;
        }
        return 0;
    }


    /**
     * Formula: 1B + 2*(2B) + 3*(3B) + 4*(HR)
     * Batter: The total number of bases i.e. one base for single, two bases for double.
     *
     * @param array $s
     * @return int
     */
    protected static function tb(array $s)
    {
        return $s['h'] - $s['2b'] - $s['3b'] - $s['hr'] + 2 * $s['2b'] + 3 * $s['3b'] + 4 * $s['hr'];
    }
    
    /**
     * Formula: H / AB
     * Batter: the average number of hits by a batter defined by hits divided by at bats.
     * Pitcher: the total number of hits allowed by the pitcher divided by the total number of opponent at-bats.
     *
     * @param array $s
     * @return float
     */
    protected static function avg(array $s)
    {
        return $s['ab'] > 0 ? $s['h'] / $s['ab'] : 0;
    }
    
    /**
     * Formula: H / AB
     * Batter: the average number of hits by a batter defined by hits divided by at bats.
     * Pitcher: the total number of hits allowed by the pitcher divided by the total number of opponent at-bats.
     *
     * @param array $s
     * @return float
     */
    protected static function oavg(array $s)
    {
        return ($s['tbf'] - $s['bb'] - $s['hbp'] - $s['sf'] - $s['sh']) > 0 ? $s['h'] / ($s['tbf'] - $s['bb'] - $s['hbp'] - $s['sf'] - $s['sh']) : 0;
    }

     /**
     * Formula: (H + BB + HBP) / (AB + BB + HBP + SF)
     * Batter: the number of times each batter reaches base by hit, walk or hit by pitch, divided by plate appearances including at-bats, walks, hit by pitch and sacrifice flies.
     * Pitcher: on base percentage against a pitcher.
     *
     * @param array $s
     * @return float
     */
    protected static function obp(array $s)
    {
        $d = $s['ab'] + $s['bb'] + $s['hbp'] + $s['sf'];
        if ($d > 0)
        {
            return ($s['h'] + $s['bb'] + $s['hbp']) / $d;
        }
        return 0;
    }
    
     /**
     * Formula: (H + BB + HBP) / (AB + BB + HBP + SF)
     * Batter: the number of times each batter reaches base by hit, walk or hit by pitch, divided by plate appearances including at-bats, walks, hit by pitch and sacrifice flies.
     * Pitcher: on base percentage against a pitcher.
     *
     * @param array $s
     * @return float
     */
    protected static function oobp(array $s)
    {
        $d = $s['tbf'] - $s['sh'];
        if ($d > 0)
        {
            return ($s['h'] + $s['bb'] + $s['hbp']) / $d;
        }
        return 0;
    }

    /**
     * Formula: TB / AB
     * Batter: slugging Percentage. The measure of the power of a hitter, total bases divided by at bats.
     *
     * @param array $s
     * @return float
     */
    protected static function slg(array $s)
    {
        return $s['ab'] > 0 ? static::tb($s) / $s['ab'] : 0;
    }
    
    /**
     * Formula: TB / AB
     * Batter: slugging Percentage. The measure of the power of a hitter, total bases divided by at bats.
     *
     * @param array $s
     * @return float
     */
    protected static function oslg(array $s)
    {
        return ($s['tbf'] - $s['bb'] - $s['hbp'] - $s['sf'] - $s['sh']) > 0 ? static::tb($s) / ($s['tbf'] - $s['bb'] - $s['hbp'] - $s['sf'] - $s['sh']) : 0;
    }

    /**
     * Formula: OBP + SLG
     * Batter: on-base percentage plus slugging.
     *
     * @param array $s
     * @return float
     */
    protected static function ops(array $s)
    {
        return static::obp($s) + static::slg($s);
    }
    
    /**
     * Formula: 9 * ER / IP
     * Pitcher: the average number of earned runs allowed by a pitcher.
     *
     * @param array $s
     * @return float
     */
    protected static function era(array $s)
    {
        return $s['ip'] > 0 ? $s['er'] / $s['ip'] * 9 : 0;
    }
    
    /**
     * Formula: SB / (SB + CS)
     * Batter: stolen base percentage. The percentage of attempts that a base runner successfully steals a base.
     *
     * @param array $s
     * @return float
     */
    protected static function sbp(array $s)
    {
        $d = $s['sb'] + $s['cs'];
        return $d > 0 ? $s['sb'] / $d : 0;
    }

    /**
     * Formula: QS / GS
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function qsp(array $s)
    {
        return $s['gs'] > 0 ? $s['qs'] / $s['gs'] : 0;
    }

    /**
     * Formula: W / (W + L)
     * Pitcher: winning percentage.
     *
     * @param array $s
     * @return float
     */
    protected static function wlp(array $s)
    {
        $d = $s['w'] + $s['l'];
        return $d > 0 ? $s['w'] / $d : 0;
    }

    /**
     * Formula: SV / SVOPP
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function svp(array $s)
    {
        return $s['svopp'] > 0 ? $s['sv'] / $s['svopp'] : 0;
    }
    
    /**
     * Formula: IRS / IR
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function irsp(array $s)
    {
        return $s['ir'] > 0 ? $s['irs'] / $s['ir'] : 0;
    }

    /**
     * Formula: H / IP * 9
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function h9(array $s)
    {
        return $s['ip'] > 0 ? $s['h'] / $s['ip'] * 9 : 0;
    }
    /**
     * Formula: K / IP * 9
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function k9(array $s)
    {
        return $s['ip'] > 0 ? $s['k'] / $s['ip'] * 9 : 0;
    }
    /**
     * Formula: BB / IP * 9
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function bb9(array $s)
    {
        return $s['ip'] > 0 ? $s['bb'] / $s['ip'] * 9 : 0;
    }
    /**
     * Formula: HR / IP * 9
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function hr9(array $s)
    {
        return $s['ip'] > 0 ? $s['hr'] / $s['ip'] * 9 : 0;
    }

    /**
     * Formula: BR / IP * 9
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function br9(array $s)
    {
        return $s['ip'] > 0 ? ($s['h'] + $s['bb'] + $s['hbp']) / $s['ip'] * 9 : 0;
    }

    /**
     * Formula: RSS / IP * 9
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function rs9(array $s)
    {
        return $s['ip'] > 0 ? $s['rss'] / $s['ip'] * 9 : 0;
    }
    /**
     * Formula: K / BB
     * Pitcher:
     *
     * @param array $s
     * @return float
     */
    protected static function kbb(array $s)
    {
        return $s['bb'] > 0 ? $s['k'] / $s['bb'] : 0;
    }
    /**
     * Formula: (TC - E) / TC
     * Defense:
     *
     * @param array $s
     * @return float
     */
    protected static function fav(array $s)
    {
        return $s['tc'] > 0 ? ($s['tc'] - $s['e']) / $s['tc']  : 0;
    }
    /**
     * Formula: A / INN * 9
     * Defense:
     *
     * @param array $s
     * @return float
     */
    protected static function a9(array $s)
    {
        return $s['inn'] > 0 ? $s['a'] / $s['inn'] * 9 : 0;
    }
    /**
     * Formula: TC / INN * 9
     * Defense:
     *
     * @param array $s
     * @return float
     */
    protected static function tc9(array $s)
    {
        return $s['inn'] > 0 ? $s['tc'] / $s['inn'] * 9 : 0;
    }
    /**
     * Converts stats record object to an array.
     *
     * @param mixed $s The stats record object.
     * @param array $fields The mandatory stats fields that will be used to calculate stats.
     * @return array
     */
    private static function normalize($s, array $fields)
    {
        if ($s instanceof Model)
        {
            $s = $s->toArray();
        }
        else if ($s instanceof \stdClass)
        {
            $s = (array)$s;
        }
        else if (!is_array($s))
        {
            throw new \InvalidArgumentException('The stats record must be an array.');
        }
        $res = [];
        foreach ($fields as $field)
        {
            if (isset($s[$field]))
            {
                $res[$field] = $s[$field];
            }
            else if (isset($s['batting_' . $field]))
            {
                $res[$field] = $s['batting_' . $field];
            }
            else if (isset($s['pitching_' . $field]))
            {
                $res[$field] = $s['pitching_' . $field];
            }
            else
            {
                $res[$field] = 0;
            }
        }
        return $res;
    }

    /**
     * Calculate the stats in row
     *
     * @param $row
     * @param $type
     */
    public static function calcStatsRow(&$row, $type = null)
    {
        $formulas = static::getFieldsMap();
        if (in_array($type, ['scaled', 'projected'])) {
            $formulas = array_merge($formulas, [
                'csp' => 'csp',
                'ccsp' => 'ccsp',
            ]);
        }
    
        // sort values, first without calc.
        uksort($row, function ($field) use ($formulas)
        {
            return isset($formulas[$field]);
        });
    
        $types = static::getFieldsType();

        foreach ($row as $field => &$value) { 
            if (isset($formulas[$field]) || in_array($field, $formulas)) {
                $key = isset($formulas[$field]) ? $field : array_search($field, $formulas);
                $value = call_user_func_array([StatsCalculator::class, $formulas[$key]], [$row]);
            }
            
            if (isset($types[$field])) {
                switch ($types[$field]) {
                    case 'int':
                        $value = (int)round($value, 0);
                        break;
                    default:
                        break;
                }
            }
        }
        unset($value);
    }
}