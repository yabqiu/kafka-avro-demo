package cc.unmi;

import cc.unmi.serialization.AvroDeserializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class Consumer<T extends SpecificRecordBase> {

    private KafkaConsumer<String, T> consumer = new KafkaConsumer<>(getProperties());

    public List<T> receive(Topic topic) {
//        TopicPartition partition = new TopicPartition(topic.topicName, 0);
        consumer.subscribe(Collections.singletonList(topic.topicName));
//        consumer.assign(Collections.singletonList(partition));
        ConsumerRecords<String, T> records = consumer.poll(10);

        return StreamSupport.stream(records.spliterator(), false)
                .map(ConsumerRecord::value).collect(Collectors.toList());
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "DemoConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                AvroDeserializer.class.getName());
        return props;
    }
}
