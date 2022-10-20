<?php

namespace Mikloukho\SymfonyMessengerRedis;

use Mikloukho\SymfonyMessengerRedis\Stamp\DebounceStamp;
use Redis;
use Symfony\Component\Messenger\Envelope;
use Symfony\Component\Messenger\Stamp\DelayStamp;
use Symfony\Component\Messenger\Stamp\TransportMessageIdStamp;
use Symfony\Component\Messenger\Transport\Receiver\MessageCountAwareInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;
use Symfony\Component\Messenger\Exception\{InvalidArgumentException, TransportException};

final class RedisTransport implements TransportInterface, MessageCountAwareInterface
{
    private $serializer;
    private $redis;
    private $queue;
    private $connectParams;
    private $configureConnection;
    private $isConnected;

    public function __construct(
        SerializerInterface $serializer,
        Redis $redis,
        string $queue,
        array $connectParams = ['127.0.0.1', 6379],
        callable $configureConnection = null
    ) {
        $this->serializer = $serializer;
        $this->redis = $redis;
        $this->queue = $queue;
        $this->connectParams = $connectParams;
        $this->configureConnection = $configureConnection;
        $this->isConnected = false;
    }

    public static function fromDsn(SerializerInterface $serializer, string $dsn, array $options = []) {
        $parsedUrl = \parse_url($dsn);
        if (!$parsedUrl) {
            throw new InvalidArgumentException(sprintf('The given Redis DSN "%s" is invalid.', $dsn));
        }

        \parse_str($parsedUrl['query'] ?? '', $query);

        if (($options['queue'] ?? $query['queue'] ?? null) === null) {
            throw new InvalidArgumentException('The queue option must be included in the query parameters or configuration options.');
        }

        $host = $parsedUrl['host'] ?? '127.0.0.1';
        if ($parsedUrl['scheme'] === 'rediss') {
            $host = 'tls://'.$host;
        }

        return new self(
            $serializer,
            new Redis(),
            $options['queue'] ?? $query['queue'],
            [$host, intval($parsedUrl['port'] ?? 6379)],
            function(Redis $conn) use ($query, $parsedUrl, $options) {
                $auth = $options['password'] ?? $parsedUrl['pass'] ?? null;
                if ($auth) {
                    $conn->auth($auth);
                }
                $db = $parsedUrl['path'] ?? $options['db'] ?? $query['db'] ?? null;
                if ($db !== null) {
                    $db = intval(ltrim($db, '/'));
                    $conn->select($db);
                }
            }
        );
    }

    /** @return Envelope[] */
    public function get(): iterable {
        $this->connect();
        $this->redis->clearLastError();
        $encodedMessage = $this->redis->eval($this->popLuaScript(), [
            $this->getUniqueHashName(),
            $this->getDelayedSetName(),
            $this->queue,
            $this->getProcessingQueue(),
            microtime(true) * 1000,
        ], 4);

        if ($this->redis->getLastError()) {
            throw new TransportException('Failed to retrieve message from queue. Redis Error: ' . $this->redis->getLastError());
        }

        if (!$encodedMessage) {
            return [];
        }

        $res = json_decode($encodedMessage, true);
        $message = isset($res[0], $res[1]) ? ['body' => $res[0], 'headers' => $res[1]] : $res;
        $envelope = $this->serializer->decode($message);
        return [$envelope->with(new TransportMessageIdStamp($encodedMessage))];
    }

    public function ack(Envelope $env): void {
        $this->connect();
        $transportIdStamp = $env->last(TransportMessageIdStamp::class);
        $this->clearMessageFromProcessingQueue($this->redis, $transportIdStamp ? $transportIdStamp->getId() : $this->encodeEnvelope($env));
    }

    public function reject(Envelope $env): void {
        $this->connect();
        $transportIdStamp = $env->last(TransportMessageIdStamp::class);
        $this->clearMessageFromProcessingQueue($this->redis, $transportIdStamp ? $transportIdStamp->getId() : $this->encodeEnvelope($env));
    }

    public function send(Envelope $env): Envelope {
        $this->connect();

        [$message, $uniqueId] = $this->encodeEnvelopeWithUniqueId($env);
        $res = $this->redis->eval($this->pushLuaScript(), [
            $this->getUniqueHashName(),
            $this->getDelayedSetName(),
            $uniqueId,
            $this->getStampDelayTimestampMs($env),
            $message,
        ], 2);
        if ($this->redis->getLastError()) {
            throw new TransportException('Failed to push message onto queue. Redis Error: ' . $this->redis->getLastError());
        }

        return $env;
    }

    private function getStampDelayTimestampMs(Envelope $env): ?float {
        $stamp = $env->last(DebounceStamp::class) ?: $env->last(DelayStamp::class);
        $delay = (microtime(true) * 1000);
        $delay += $stamp ? $stamp->getDelay() : 0;
        return $delay;
    }

    private function encodeEnvelope(Envelope $env): string {
        return $this->encodeEnvelopeWithUniqueId($env)[0];
    }

    private function encodeEnvelopeWithUniqueId(Envelope $env): array {
        $encoded = $this->serializer->encode($env);
        /** @var DebounceStamp|null $stamp */
        $stamp = $env->last(DebounceStamp::class);
        $uniqueId = $stamp ? strval($stamp->getId()) : md5($encoded['body']);
        return [\json_encode(array_merge($encoded, [
            'uniqueId' => $uniqueId,
        ])), $uniqueId];
    }

    private function isDebounceStampExist(Envelope $env): bool
    {
        return \boolval($env->last(DebounceStamp::class));
    }

    public function getMessageCount(): int {
        $this->connect();
        $pipe = $this->redis->multi(Redis::PIPELINE);
        $pipe->lLen($this->queue);
        $pipe->zCount($this->getDelayedSetName(), '-inf', '+inf');
        return array_sum(array_map('intval', $pipe->exec()));
    }

    private function connect(): void {
        if ($this->isConnected) {
            return;
        }
        $this->isConnected = true;
        $this->redis->connect(...$this->connectParams);
        if ($configureConnection = $this->configureConnection) {
            $configureConnection($this->redis);
        }
    }

    private function clearMessageFromProcessingQueue(Redis $redis, string $message) {
        return $redis->lRem($this->getProcessingQueue(), $message, 1);
    }

    private function getProcessingQueue(): string {
        return $this->queue . '_processing';
    }

    private function getUniqueHashName(): string {
        return $this->queue . ':unique';
    }

    private function getDelayedSetName(): string {
        return $this->queue . ':delayed';
    }

    /** return the lua script for pushing an item into the queue */
    private function pushLuaScript(): string {
        return <<<LUA
local uniqueHashKey = KEYS[1]
local delaySetKey = KEYS[2]
local uniqueId = ARGV[1]
local delayTimestampMs = ARGV[2]
local message = ARGV[3]
redis.call("ZADD", delaySetKey, delayTimestampMs, uniqueId)
redis.call("HSET", uniqueHashKey, uniqueId, message)
return 2
LUA;
    }

    /** return the lua script for popping items off of the queue */
    private function popLuaScript() {
        return <<<LUA
local uniqueHashKey = KEYS[1]
local delayedSetKey = KEYS[2]
local queueKey = KEYS[3]
local processingQueueKey = KEYS[4]
local currentTimestampMs = ARGV[1]

-- first, we need to enqueue any delayed messages that are ready to be processed
local readyMessages = redis.call("ZRANGEBYSCORE", delayedSetKey, "-inf", currentTimestampMs)
for key,uniqueId in pairs(readyMessages) do
  redis.call("LPUSH", queueKey, redis.call("HGET", uniqueHashKey, uniqueId))
  redis.call("ZREM", delayedSetKey, uniqueId)
end 

local message = redis.call("RPOPLPUSH", queueKey, processingQueueKey)
if message == false then
  return false
end

local decodedMessage = cjson.decode(message)
if type(decodedMessage["uniqueId"]) == "string" then
  redis.call("HDEL", uniqueHashKey, decodedMessage["uniqueId"])
end

return message
LUA;

    }
}
