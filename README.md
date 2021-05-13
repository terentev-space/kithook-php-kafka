# KitHook PHP Kafka

[![Latest Version](https://img.shields.io/github/release/terentev-space/kithook-php-kafka.svg?style=flat-square)](https://github.com/terentev-space/kithook-php-kafka/releases)
[![Software License](https://img.shields.io/badge/license-Apache_2.0-brightgreen.svg?style=flat-square)](LICENSE)
[![Total Downloads](https://img.shields.io/packagist/dt/terentev-space/kithook-php-kafka.svg?style=flat-square)](https://packagist.org/packages/terentev-space/kithook-php-kafka)

#### 🚧 Attention: the project is currently under development! 🚧

**Note:** Before using it, you need to deploy and configure [KitHook](https://github.com/terentev-space/kithook)

This project acts as an extension of [kithook-php](https://github.com/terentev-space/kithook-php) and allows you to use the Kafka queue manager to send messages to [KitHook](https://github.com/terentev-space/kithook)

## Install

Via Composer

```bash
$ composer require terentev-space/kithook-php-kafka
```

## Usage

Data:
```php
$yourWebhookUri = 'https://example.com';
$yourWebhookId = 'myId-123';

$yourElseContentType = 'application/xml';

// Json
$yourWebhookJsonData = [
    'anything'
];
// Form
$yourWebhookFormData = [
    'param1' => 'value1',
];
// Query
$yourWebhookQueryData = [
    'param1' => 'value1',
];
// Xml
$yourWebhookXmlData = <<<XML
<?xml version="1.0" encoding="UTF-8"?>
<example>
<!-- ... -->
</example>
XML;

$yourClientConfig = [
    \KitHookKafka\Client::CONFIG_KAFKA_BROKER_LIST => 'localhost:9092',
    \KitHookKafka\Client::CONFIG_KAFKA_GROUP => 'default',
    \KitHookKafka\Client::CONFIG_KAFKA_TOPIC => 'kithook',
];
```

Examples:
```php
$client = isset($yourClientConfig) ? 
new \KitHookKafka\Client($yourClientConfig) : // Use config
new \KitHookKafka\Client(); // Use env

// Simple HTTP Put Json
$client->sendHttpPutJson($yourWebhookUri, $yourWebhookJsonData, $yourWebhookId);
// Simple HTTP Post Form
$client->sendHttpPostForm($yourWebhookUri, $yourWebhookFormData, $yourWebhookId);
// Simple HTTP Get Query
$client->sendHttpGetEmpty($yourWebhookUri . '?' . http_build_query($yourWebhookQueryData), $yourWebhookId);

// Build
$content = $client->contentBuilder()->makeHttp()
    // OR
    ->withType(\KitHook\Entities\Messages\Contents\QueueHttpMessageContent::TYPE_ELSE)
    ->withData($yourWebhookXmlData)
    ->withFormatForElseType($yourElseContentType)
    // OR
    ->fillElse($yourWebhookXmlData, $yourElseContentType)
    // FINALLY
    ->build();

$message = $client->messageBuilder()->makeHttp()
    // OR
    ->withId($yourWebhookId)
    ->withUri($yourWebhookUri)
    ->withMethod(\KitHook\Entities\Messages\QueueHttpMessage::METHOD_POST)
    ->withContent($content)
    // OR
    ->fillPost($yourWebhookUri, $yourWebhookId, $content, [/* headers */], [/* properties */])
    // OR
    ->fillPost($yourWebhookUri, $yourWebhookId)
    ->makeContent()
    ->fillForm($yourWebhookFormData)
    ->buildFromMessage()
    // FINALLY
    ->build();

$client->send($message);
```

## Credits

- [Ivan Terentev](https://github.com/terentev-space)
- [All Contributors](https://github.com/terentev-space/kithook-php-kafka/contributors)

## License

The Apache 2.0 License (Apache-2.0). Please see [License File](LICENSE) for more information.
