<?php
$group = $_SERVER['argv'][1];
$brokers = $_SERVER['argv'][2];
$topics = array_slice($_SERVER['argv'], 3);

echo "group: {$group}\n";
echo "brokers: {$brokers}\n";
echo "topics: " . implode(' ', $topics) . "\n";

$kafka_conf = new RdKafka\Conf();
$kafka_conf->set('group.id', $group);
// Initial list of Kafka brokers
$kafka_conf->set('metadata.broker.list', $brokers);
$kafka_conf->setRebalanceCb(function (RdKafka\KafkaConsumer $kafka, $err, array $partitions = null) {
    switch ($err) {
        case RD_KAFKA_RESP_ERR__ASSIGN_PARTITIONS:
            echo "assign\n";
            $kafka->assign($partitions);
            break;
         case RD_KAFKA_RESP_ERR__REVOKE_PARTITIONS:
            echo "revoke\n";
            $kafka->assign(NULL);
            break;
         default:
            throw new \Exception($err);
    }
});

$topicConf = new RdKafka\TopicConf();
// Set where to start consuming messages when there is no initial offset in
// offset store or the desired offset is out of range.
// 'smallest': start from the beginning
$topicConf->set('auto.offset.reset', 'latest');
// Set the configuration to use for subscribed/assigned topics
$kafka_conf->setDefaultTopicConf($topicConf);

$consumer = new RdKafka\KafkaConsumer($kafka_conf);
$consumer->subscribe($topics);

$quit = false;

pcntl_signal(SIGTERM/*15*/, function($sig) use(&$quit) {
    $quit = true;
});

pcntl_signal(SIGINT/*2 or ctrl+c*/, function($sig) use(&$quit) {
    $quit = true;
});

$result = [
    'count' => 0,
];

while (!$quit) {
    pcntl_signal_dispatch();

    $message = $consumer->consume(100);
    
    switch ($message->err) {
        case RD_KAFKA_RESP_ERR_NO_ERROR:
            $data = json_decode($message->payload, true)['data'];
            
            $device_id = $data['device_id'];
            $imei = $data['imei'];
            $imei2 = $data['imei2'];
            $oaid = $data['oaid'];
            $caid = $data['caid'] ?? null;
            $caid2 = $data['caid2'] ?? null;
            $app_uuid = $data['app_uuid'] ?? null;
            
            @file_put_contents("php://stderr", "----------------" . memory_get_usage() / 1024 / 1024);
            
            $result['count'] += 1;
            if($result['count'] >= 1000000) break;
            
            $consumer->commit();
            break;
        case RD_KAFKA_RESP_ERR__PARTITION_EOF:
            @file_put_contents("php://stderr", sprintf("%s(%d)", $message->errstr(), $message->err));
            break;
        case RD_KAFKA_RESP_ERR__TIMED_OUT:
            @file_put_contents("php://stderr", sprintf("%s(%d)", $message->errstr(), $message->err));
            break;
        default:
            @file_put_contents("php://stderr", sprintf("%s(%d)", $message->errstr(), $message->err));
            break;
    }
}

var_dump($result);
