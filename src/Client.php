<?php

namespace KitHookKafka;

use Enqueue\RdKafka\RdKafkaConnectionFactory;
use Enqueue\RdKafka\RdKafkaContext;
use Interop\Queue\Context;
use KitHook\Adapter;
use KitHook\Builders\MessageBuilder\Builder as MessageBuilder;
use KitHook\Builders\MessageBuilder\ContentBuilder\Builder as ContentBuilder;
use KitHook\Entities\Messages\QueueMessage;
use KitHook\Interfaces\ClientInterface;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;
use RuntimeException;
use Throwable;

/**
 * Class Client
 * @package KitHookKafka
 * @method MessageBuilder messageBuilder()
 * @method ContentBuilder contentBuilder()
 * @method void sendHttpGetEmpty(string $uri, ?string $id = null)
 * @method void sendHttpPostEmpty(string $uri, ?string $id = null)
 * @method void sendHttpPutEmpty(string $uri, ?string $id = null)
 * @method void sendHttpDeleteEmpty(string $uri, ?string $id = null)
 * @method void sendHttpGetJson(string $uri, mixed $data, ?string $id = null)
 * @method void sendHttpPostJson(string $uri, mixed $data, ?string $id = null)
 * @method void sendHttpPutJson(string $uri, mixed $data, ?string $id = null)
 * @method void sendHttpDeleteJson(string $uri, mixed $data, ?string $id = null)
 * @method void sendHttpGetForm(string $uri, array $data, ?string $id = null)
 * @method void sendHttpPostForm(string $uri, array $data, ?string $id = null)
 * @method void sendHttpPutForm(string $uri, array $data, ?string $id = null)
 * @method void sendHttpDeleteForm(string $uri, array $data, ?string $id = null)
 */
class Client implements ClientInterface
{
    /** @var array */
    private $config;

    /** @var LoggerInterface */
    private $logger;

    /** @var Adapter */
    private $adapter;

    /** @var bool */
    private $isAlreadyInit = false;
    public const CONFIG_KAFKA_ENV = 'environment';

    public const CONFIG_KAFKA_BROKER_LIST = 'broker_list';
    public const CONFIG_KAFKA_GROUP = 'group';
    public const CONFIG_KAFKA_TOPIC = 'topic';

    public const ENV_KAFKA_BROKER_LIST = 'KAFKA_BROKER_LIST';
    public const ENV_KAFKA_GROUP = 'KAFKA_GROUP';
    public const ENV_KAFKA_TOPIC = 'KAFKA_TOPIC';

    private $configKafkaBrokerList;
    private $configKafkaGroup;
    private $configKafkaTopic;

    /** @var RdKafkaConnectionFactory|null */
    private $connectionFactory;
    /** @var RdKafkaContext|Context|null */
    private $context;

    /**
     * Client constructor.
     *
     * @param array $config
     * @param LoggerInterface|null $logger
     */
    public function __construct(
        array $config = [],
        ?LoggerInterface $logger = null
    ) {
        $this->config = $config;
        $this->logger = $logger;
    }

    /**
     * @param QueueMessage $message
     * @throws Throwable
     */
    public function send(QueueMessage $message): void
    {
        $this->initIfNeed();
        $this->connectIfNeed();

        try {
            $queueMessage = $this->context->createMessage($message->jsonSerialize());
            $topic = $this->context->createTopic($this->configKafkaTopic);
            $this->context->createProducer()->send($topic, $queueMessage);
        } catch (Throwable $exception) {
            $this->logger->warning($exception->getMessage());
            throw $exception;
        }
    }

    private function initIfNeed(): void
    {
        if ($this->isAlreadyInit) {
            return;
        }

        $environment = $this->config[self::CONFIG_KAFKA_ENV] ?? null;

        if ($environment === null) {
            $environment = $_ENV ?: getenv();
        }

        $this->checkAndFillConfig($environment);

        $this->logger = $this->logger ?? new NullLogger();

        $this->isAlreadyInit = true;
    }

    private function connectIfNeed(): void
    {
        if (!$this->connectionFactory instanceof RdKafkaConnectionFactory) {
            $this->connectionFactory = new RdKafkaConnectionFactory([
                'global' => [
                    'group.id' => $this->configKafkaGroup,
                    'metadata.broker.list' => $this->configKafkaBrokerList,
                    'enable.auto.commit' => 'false',
                ],
                'topic' => [
                    'auto.offset.reset' => 'beginning',
                ],
            ]);
        }

        if ($this->context instanceof Context) {
            $this->context = $this->connectionFactory->createContext();
        }
    }

    private function checkAndFillConfig(array $environment): void
    {
        $this->configKafkaBrokerList = $this->config[self::CONFIG_KAFKA_BROKER_LIST] ?? $environment[self::ENV_KAFKA_BROKER_LIST] ?? null;
        $this->configKafkaGroup = $this->config[self::CONFIG_KAFKA_GROUP] ?? $environment[self::ENV_KAFKA_GROUP] ?? null;
        $this->configKafkaTopic = $this->config[self::CONFIG_KAFKA_TOPIC] ?? $environment[self::ENV_KAFKA_TOPIC] ?? null;

        if (!$this->configKafkaBrokerList) {
            throw new RuntimeException('Config parameter "broker_list" is required');
        }
        if (!$this->configKafkaGroup) {
            throw new RuntimeException('Config parameter "group" is required');
        }
        if (!$this->configKafkaTopic) {
            throw new RuntimeException('Config parameter "topic" is required');
        }
    }

    public function __call($name, $arguments)
    {
        if (!$this->adapter instanceof Adapter) {
            $this->adapter = new Adapter($this);
        }

        if (method_exists($this->adapter, $name)) {
            return $this->adapter->$name(...$arguments);
        }

        throw new RuntimeException(sprintf('Unknown method "%s"', $name));
    }

    public function __destruct()
    {
        if ($this->context instanceof Context) {
            $this->context->close();
        }
    }
}
