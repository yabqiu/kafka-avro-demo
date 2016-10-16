package cc.unmi;

import cc.unmi.serialization.AvroDeserializer;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

public class Consumer<T extends SpecificRecordBase> {

    public List<T> receive(Topic topic) {
        Properties props = getProperties();
        KafkaConsumer<String, T> consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(topic.topicName));

        ConsumerRecords<String, T> records = consumer.poll(10);

        List<T> data = new ArrayList<T>(records.count());
        records.forEach(record -> {
            data.add(record.value());
        });

        return data;
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
