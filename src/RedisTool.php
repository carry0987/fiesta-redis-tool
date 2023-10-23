<?php
namespace carry0987\Fiesta;

use Exception;
use RedisException;

class RedisTool
{
    private $redis = null;
    private $retryTimes;

    public function __construct(string $host, int $port, string $pwd, mixed $database, int $retryTimes = 3)
    {
        if (!class_exists('Redis')) throw new Exception('Class Redis does not exist !');
        $this->retryTimes = $retryTimes;
        try {
            $this->redis = new \Redis();
            $count = 0;
            while (!$this->redis->connect($host, $port) && $count < $this->retryTimes) {
                $count++;
            }
            if (!$this->redis->connect($host, $port)) {
                throw new Exception('Unable to connect to Redis');
            }
            $this->redis->auth($pwd);
            $this->redis->select($database);
        } catch (RedisException $e) {
            throw new Exception($e->getMessage());
        }
    }

    private function isConnected()
    {
        try {
            $status = $this->redis->ping();
            return $status == '+PONG';
        } catch (RedisException $e) {
            throw new Exception($e->getMessage());
        }
    }

    public function getRedis()
    {
        return $this->redis;
    }

    public function setValue($key, $value, int $ttl = 86400)
    {
        if (!$this->isConnected()) return false;
        if ($ttl !== null) {
            $status = $this->redis->setex($key, $ttl, $value);
        } else {
            $status = $this->redis->set($key, $value);
        }
        return $status === true;
    }

    public function setIndex(string $indexKey, string $value) 
    {
        if (!$this->isConnected()) return false;
        return $this->setValue($indexKey, $value);
    }

    public function setHashValue($hash, $key, $value, int $ttl = 86400)
    {
        if (!$this->isConnected()) return false;
        $this->redis->multi();
        $this->redis->hSet($hash, $key, $value);
        if ($ttl !== null) { 
            $this->redis->expire($hash, $ttl);
        }
        $status = $this->redis->exec();
        return $status !== false;
    }

    public function getValue($key)
    {
        return $this->redis->get($key);
    }

    public function getHashValue($hash, $key)
    {
        return $this->redis->hGet($hash, $key);
    }

    public function getAllHash($hash)
    {
        return $this->redis->hGetAll($hash);
    }

    public function deleteValue($key)
    {
        if (!$this->isConnected()) return false;
        return (bool) $this->redis->del($key);
    }

    public function exists($key)
    {
        if (!$this->isConnected()) return false;
        return (bool) $this->redis->exists($key);
    }

    public function flushDatabase()
    {
        return $this->redis->flushDb();
    }

    public function keys($pattern)
    {
        if (!$this->isConnected()) return array();

        $it = null; /* Initialize our iterator to NULL */

        $redis = $this->redis;
        $redis->setOption(\Redis::OPT_SCAN, \Redis::SCAN_RETRY); /* retry when we get no keys back */

        $keys = array();
        while ($array = $redis->scan($it, $pattern)) {
            foreach ($array as $key) {
                $keys[] = $key;
            }
        }
        return $keys;
    }
}
