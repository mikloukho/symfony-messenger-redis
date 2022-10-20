<?php

namespace Mikloukho\SymfonyMessengerRedis;

use Symfony\Component\Messenger\Transport\Receiver\ReceiverInterface;
use Symfony\Component\Messenger\Transport\Sender\SenderInterface;
use Symfony\Component\Messenger\Transport\Serialization\SerializerInterface;
use Symfony\Component\Messenger\Transport\TransportFactoryInterface;
use Symfony\Component\Messenger\Transport\TransportInterface;

final class RedisTransportFactory implements TransportFactoryInterface
{
    public function createTransport(string $dsn, array $options, SerializerInterface $serializer): TransportInterface {
        return RedisTransport::fromDsn($serializer, $dsn, $options);
    }

    /**
     * Symfony's redis factory also matches on the redis:// prefix, so to support using
     * both redis adapters at the same time, the `use_lists` option allows you to opt out
     * of this implementation.
     */
    public function supports(string $dsn, array $options): bool {
        return (strpos($dsn, 'redis://') === 0);
    }
}
