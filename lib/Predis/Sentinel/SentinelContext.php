<?php

/*
 * This file is part of the Predis package.
 *
 * (c) Daniele Alessandri <suppakilla@gmail.com>
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 */

namespace Predis\Sentinel;

use Predis\Helpers;
use Predis\ClientException;
use Predis\ConnectionParameters;
use Predis\Profile\ServerProfile;
use Predis\PubSub\AbstractPubSubContext;
use Predis\Connection\StreamConnection;

/**
 * Main class that exposes the most high-level interface to Redis Sentinel.
 *
 * @author Daniele Alessandri <suppakilla@gmail.com>
 */
class SentinelContext extends AbstractPubSubContext
{
    protected $profile;
    protected $connection;

    /**
     * @param string|array $parameters Connection parameters
     */
    public function __construct($parameters)
    {
        $parameters = new ConnectionParameters($parameters);

        $this->connection = new StreamConnection($parameters);
        $this->profile = ServerProfile::get('dev');
    }

    /**
     * {@inheritdoc}
     */
    protected function writeCommand($method, $arguments)
    {
        $arguments = Helpers::filterArrayArguments($arguments);
        $command = $this->profile->createCommand($method, $arguments);
        $this->connection->writeCommand($command);
    }

    /**
     * {@inheritdoc}
     */
    protected function disconnect()
    {
        $this->connection->disconnect();
    }

    /**
     * {@inheritdoc}
     */
    protected function getValue()
    {
        $response = $this->connection->read();

        switch ($response[0]) {
            case self::SUBSCRIBE:
            case self::PSUBSCRIBE:
            case self::UNSUBSCRIBE:
            case self::PUNSUBSCRIBE:
                if ($response[2] === 0) {
                    $this->invalidate();
                }

                return (object) array(
                    'kind'    => $response[0],
                    'channel' => $response[1],
                    'payload' => $response[2],
                );

            case self::MESSAGE:
                return $this->handleMessage($response[1], $response[2]);

            case self::PMESSAGE:
                return $this->handleMessage($response[2], $response[3]);

            default:
                $message = "Received an unknown message type {$response[0]} inside of a pubsub context";
                throw new ClientException($message);
        }
    }

    /**
     * Handles an incoming (P)MESSAGE.
     *
     * @param string $channel Channel name
     * @param string $payload Payload string
     * @return object
     */
    protected function handleMessage($channel, $payload)
    {
        $message = array(
            'kind'    => self::MESSAGE,
            'channel' => $channel,
        );

        if ($channel === '+tilt' || $channel === '-tilt') {
            return (object) $message;
        }

        $method = $channel === 'switch-master' ? 'parseSwitch' : 'parseDetails';
        $message['instance'] = $this->$method($payload);

        return (object) $message;
    }

    /**
     * Parses the payload string coming from a (P)MESSAGE with the instance
     * details into a PHP object.
     *
     * @param string $payload Payload string containing instance details
     * @return object
     */
    protected function parseDetails($payload)
    {
        @list($type, $name, $host, $port, $master) = explode(' ', $payload, 5);

        $details = array(
            'type' => $type,
            'name' => $name,
            'host' => $host,
            'port' => (int) $port,
        );

        if ($type !== 'master') {
            list($name, $host, $post) = explode(' ', $master, 3);

            $details['master'] = (object) array(
                'name' => $name,
                'host' => $host,
                'port' => (int) $port,
            );
        }

        return (object) $details;
    }
}
